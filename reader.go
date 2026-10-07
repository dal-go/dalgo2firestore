package dalgo2firestore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"google.golang.org/api/iterator"
)

var _ dal.Reader = (*firestoreReader)(nil)

var docIteratorNext = func(it *firestore.DocumentIterator) (*firestore.DocumentSnapshot, error) {
	return it.Next()
}

type firestoreReader struct {
	i           int // iteration
	query       dal.Query
	docIterator *firestore.DocumentIterator
}

func (d *firestoreReader) Close() error {
	return nil
}

func (d *firestoreReader) Next() (record dalrecord.Record, err error) {
	switch q := d.query.(type) {
	case dal.StructuredQuery:
		if limit := d.query.Limit(); limit > 0 && d.i >= limit {
			return nil, dal.ErrNoMoreRecords
		}
		if record = q.IntoRecord(); record == nil {
			from := q.From()
			base := from.Base()
			idKind := q.IDKind()
			if idKind == reflect.Invalid {
				// Queries without IntoRecord and without an ID kind (e.g. column
				// projection or GROUP BY aggregation returning map-shaped records)
				// are not implemented by this adapter. Report it per the dalgo
				// capability contract instead of panicking in record.NewRecordWithIncompleteKey.
				return nil, fmt.Errorf("%w: query without IntoRecord and ID kind (e.g. column projection or aggregation) is not supported by dalgo2firestore", dal.ErrNotSupported)
			}
			record = dalrecord.NewRecordWithIncompleteKey(base.Name(), idKind, nil)
		}

		var doc *firestore.DocumentSnapshot
		if doc, err = docIteratorNext(d.docIterator); err != nil {
			if errors.Is(err, iterator.Done) {
				err = fmt.Errorf("%w: %v", dal.ErrNoMoreRecords, err)
			}
			return record, err
		}
		record.SetError(nil)
		data := record.Data()
		rd, isDataWrapper := data.(dal.DataWrapper)
		if isDataWrapper {
			if data = rd.Data(); data == nil {
				return record, fmt.Errorf("DataWrapper.Data() returned nil")
			}
		}
		if m, isMap := data.(map[string]any); isMap {
			// DataTo requires a pointer target; a bare map[string]any is
			// filled via reference semantics (dalgo convention, matching
			// dalgo2memory and dalgo2ingitdb readers).
			for key, value := range doc.Data() {
				m[key] = value
			}
		} else if data != nil {
			if err = doc.DataTo(data); err != nil {
				return record, fmt.Errorf("failed to convert firestore document snapshot to %T: %w", data, err)
			}
		}
		key, err := keyFromFirestoreDocumentRef(doc.Ref, record.Key())
		if err != nil {
			return record, err
		}
		loaded := dalrecord.NewRecordWithData(key, record.Data())
		loaded.SetError(nil)
		if record.HasChanged() {
			loaded.MarkAsChanged()
		}
		d.i++
		return loaded, nil
	default:
		return nil, fmt.Errorf("%w: Only dal.StructuredQuery is supported, got %T", dal.ErrNotSupported, d.query)
	}

}

type firestoreDocumentPathPart struct {
	collection string
	id         string
}

// keyFromFirestoreDocumentRef builds a complete DALgo key from the actual
// reference returned by Firestore. Collection-group results have no useful
// parent information in the query's IntoRecord factory, and Firestore can
// return a subcollection document even when one of its parent documents was
// deleted. The reference chain remains authoritative in both cases.
func keyFromFirestoreDocumentRef(docRef *firestore.DocumentRef, prototype *dalrecord.Key) (*dalrecord.Key, error) {
	if docRef == nil {
		return nil, errors.New("firestore document reference is nil")
	}
	if prototype == nil {
		return nil, errors.New("record key prototype is nil")
	}

	var path []firestoreDocumentPathPart
	for current := docRef; current != nil; {
		if current.Parent == nil || current.Parent.ID == "" || current.ID == "" {
			return nil, fmt.Errorf("firestore document reference has incomplete parent path: %q", current.Path)
		}
		path = append(path, firestoreDocumentPathPart{collection: current.Parent.ID, id: current.ID})
		current = current.Parent.Parent
	}

	if path[0].collection != prototype.Collection() {
		return nil, fmt.Errorf("firestore document collection %q does not match query record collection %q", path[0].collection, prototype.Collection())
	}
	leafID, err := idFromFirestoreDocRef(docRef, prototype.IDKind)
	if err != nil {
		return nil, fmt.Errorf("failed to convert firestore document ID: %w", err)
	}

	ids := make([]any, len(path))
	ids[0] = leafID
	prototypeParent := prototype.Parent()
	for i := 1; i < len(path); i++ {
		ids[i] = path[i].id
		if prototypeParent != nil {
			if prototypeParent.Collection() == path[i].collection && fmt.Sprint(prototypeParent.ID) == path[i].id {
				ids[i] = prototypeParent.ID
			}
			prototypeParent = prototypeParent.Parent()
		}
	}

	key := (*dalrecord.Key)(nil)
	for i := len(path) - 1; i >= 0; i-- {
		key = dalrecord.NewKeyWithParentAndID(key, path[i].collection, ids[i])
	}
	key.IDKind = prototype.IDKind
	return key, nil
}

func (d *firestoreReader) Cursor() (string, error) {
	return "", dal.ErrNotImplementedYet
}

func newFirestoreReader(c context.Context, client *firestore.Client, query dal.Query) (reader *firestoreReader, err error) {
	if query == nil {
		return nil, fmt.Errorf("query is required parameter, got nil")
	}
	reader = &firestoreReader{
		query: query,
	}
	reader.docIterator, err = dalQuery2firestoreIterator(c, query, client)
	return reader, err
}

func idFromFirestoreDocRef(key *firestore.DocumentRef, idKind reflect.Kind) (id any, err error) {
	//if key.Incomplete() {
	//	return nil, errors.New("datastore key is incomplete: neither key.Name nor key.ID is setFirestore")
	//}
	switch idKind {
	case reflect.Invalid:
		return nil, errors.New("id kind is 0 e.g. 'reflect.Invalid'")
	case reflect.String:
		return key.ID, nil
	default:
		var id int
		if id, err = strconv.Atoi(key.ID); err != nil {
			return nil, fmt.Errorf("failed to autoconvert key.Name to int: %w", err)
		}
		switch idKind {
		case reflect.Int64:
			return id, nil
		case reflect.Int:
			return id, nil
		case reflect.Int32:
			return id, nil
		case reflect.Int16:
			return id, nil
		case reflect.Int8:
			return id, nil
		default:
			return key, fmt.Errorf("unsupported id type: %T=%v", idKind, idKind)
		}
	}
}
