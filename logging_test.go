package dalgo2firestore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
)

func Test_logging_helpers(t *testing.T) {
	ctx := context.Background()
	var msgs []string
	orig := Debugf
	Debugf = func(_ context.Context, format string, args ...interface{}) {
		msgs = append(msgs, fmt.Sprintf(format, args...))
	}
	defer func() { Debugf = orig }()

	records := []dal.Record{
		dal.NewRecordWithData(dal.NewKeyWithID("c", "1"), struct{}{}),
		dal.NewRecordWithData(dal.NewKeyWithID("c", "2"), struct{}{}),
	}
	logMultiRecords(ctx, "op", records /*started*/, time.Now(), nil)
	if len(msgs) == 0 {
		t.Fatalf("expected log message")
	}

	msgs = nil
	keys := []*dal.Key{dal.NewKeyWithID("c", "1"), dal.NewKeyWithID("c", "2")}
	logMultiKeys(ctx, "op2", keys /*started*/, time.Now(), nil)
	if len(msgs) == 0 || !strings.Contains(msgs[0], "op2") {
		t.Fatalf("expected log message with op2, got: %v", msgs)
	}
}
