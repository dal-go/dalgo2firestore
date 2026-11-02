package dalgo2firestore

import (
	"encoding/base64"
	"fmt"
	"strings"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/protobuf/proto"
)

// A leading empty path segment cannot be a legacy document ID or relative
// document path. Malformed tokens in this namespace never fall back to IDs.
const opaqueCursorPrefix = "//dalgo-firestore/v1/"
const maxOpaqueCursorBytes = 64 << 10

// Keep SDK round-trip failures observable without performing a database read.
var deserializeCursorQuery = firestore.Query.Deserialize

func isOpaqueCursor(cursor dal.Cursor) bool {
	return strings.HasPrefix(string(cursor), "//dalgo-firestore/")
}

// The initial implementation deliberately supports the complete explicit order
// used for indexed work queues. Implicit SDK ordering and offset pagination
// cannot be resumed with the same exact query meaning.
func supportsOpaqueCursor(q dal.StructuredQuery) bool {
	if q == nil || q.Offset() != 0 || q.StartFrom() != "" || (q.StartAfter() != "" && !isOpaqueCursor(q.StartAfter())) {
		return false
	}
	order := q.OrderBy()
	if len(order) == 0 {
		return false
	}
	for i, item := range order {
		field, ok := item.Expression().(dal.FieldRef)
		if !ok || field.IsID() != (i == len(order)-1) {
			return false
		}
	}
	return true
}

// Cursor retains the position of the last successfully returned record, also
// after EOF or Close. Before the first record it returns an empty position.
// Tokens contain private query metadata; they are not authorization capabilities.
func (d *firestoreReader) opaqueCursor() (string, error) {
	if !d.canCursor {
		return "", fmt.Errorf("%w: opaque Firestore cursor requires explicit field ordering ending in DocumentID and no offset/legacy window", dal.ErrNotSupported)
	}
	if d.lastDoc == nil {
		return "", nil
	}
	encoded, err := d.nativeQuery.StartAfter(d.lastDoc).Serialize()
	if err != nil {
		return "", fmt.Errorf("serialize Firestore continuation: %w", err)
	}
	position := new(pb.RunQueryRequest)
	// SDK Serialize guarantees a well-formed RunQueryRequest wire message.
	_ = proto.Unmarshal(encoded, position)
	if err := validateOpaqueValues(position.GetStructuredQuery().GetStartAt()); err != nil {
		return "", err
	}
	if base64.RawURLEncoding.EncodedLen(len(encoded))+len(opaqueCursorPrefix) > maxOpaqueCursorBytes {
		return "", fmt.Errorf("firestore continuation exceeds size limit")
	}
	return opaqueCursorPrefix + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func applyOpaqueCursor(q dal.StructuredQuery, native firestore.Query) (firestore.Query, error) {
	if !supportsOpaqueCursor(q) || q.StartAfter() == "" {
		return native, fmt.Errorf("%w: incompatible Firestore continuation window", dal.ErrNotSupported)
	}
	token := string(q.StartAfter())
	if len(token) > maxOpaqueCursorBytes || !strings.HasPrefix(token, opaqueCursorPrefix) {
		return native, fmt.Errorf("invalid Firestore continuation envelope")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, opaqueCursorPrefix))
	if err != nil {
		return native, fmt.Errorf("invalid Firestore continuation encoding: %w", err)
	}
	position := new(pb.RunQueryRequest)
	if err := proto.Unmarshal(encoded, position); err != nil {
		return native, fmt.Errorf("invalid Firestore continuation protobuf: %w", err)
	}
	baseBytes, err := native.Serialize()
	if err != nil {
		return native, err
	}
	base := new(pb.RunQueryRequest)
	// These bytes came from SDK Serialize, not the untrusted continuation.
	_ = proto.Unmarshal(baseBytes, base)
	got, want := position.GetStructuredQuery(), base.GetStructuredQuery()
	if got == nil || want == nil || got.StartAt == nil || got.StartAt.Before || len(got.StartAt.Values) != len(want.OrderBy) {
		return native, fmt.Errorf("firestore continuation must contain a full exclusive position")
	}
	cursor := got.StartAt
	if err := validateOpaqueValues(cursor); err != nil {
		return native, err
	}
	got.StartAt = nil
	if !proto.Equal(base, position) {
		return native, fmt.Errorf("firestore continuation belongs to a different query")
	}
	// The last explicit order field is DocumentID. Its full resource name must
	// remain in this exact project/database and collection scope.
	ref := cursor.Values[len(cursor.Values)-1].GetReferenceValue()
	prefix := base.Parent + "/"
	if !strings.HasPrefix(ref, prefix) {
		return native, fmt.Errorf("firestore continuation has a foreign document reference")
	}
	path := strings.TrimPrefix(ref, prefix)
	parts := strings.Split(path, "/")
	from := want.GetFrom()
	if len(from) != 1 || len(parts) < 2 || len(parts)%2 != 0 || parts[len(parts)-2] != from[0].CollectionId || (!from[0].AllDescendants && len(parts) != 2) {
		return native, fmt.Errorf("firestore continuation has a different collection scope")
	}
	for _, part := range parts {
		if part == "" {
			return native, fmt.Errorf("firestore continuation has an incomplete document path")
		}
	}
	// Install only the validated position into the independently rebuilt query.
	// All fields originate from valid SDK bytes or validated scalar cursor values.
	want.StartAt = proto.Clone(cursor).(*pb.Cursor)
	encoded, _ = proto.Marshal(base)
	return deserializeCursorQuery(native, encoded)
}

func validateOpaqueValues(cursor *pb.Cursor) error {
	if len(cursor.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("firestore continuation has unknown cursor fields")
	}
	for i, value := range cursor.Values {
		if value == nil || value.ValueType == nil || len(value.ProtoReflect().GetUnknown()) != 0 {
			return fmt.Errorf("firestore continuation has an invalid value")
		}
		switch v := value.ValueType.(type) {
		case *pb.Value_TimestampValue:
			if v.TimestampValue == nil || v.TimestampValue.CheckValid() != nil {
				return fmt.Errorf("firestore continuation has an invalid timestamp")
			}
		case *pb.Value_ReferenceValue:
			if i != len(cursor.Values)-1 {
				return fmt.Errorf("%w: reference-valued ordinary cursor field", dal.ErrNotSupported)
			}
		case *pb.Value_BooleanValue, *pb.Value_IntegerValue, *pb.Value_DoubleValue, *pb.Value_StringValue, *pb.Value_BytesValue, *pb.Value_NullValue:
		default:
			return fmt.Errorf("%w: composite cursor field value", dal.ErrNotSupported)
		}
	}
	return nil
}
