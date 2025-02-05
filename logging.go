package dalgo2firestore

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"strings"
)

var Debugf func(ctx context.Context, format string, args ...interface{}) = nil

func logMultiRecords(ctx context.Context, operation string, records []dal.Record) {
	if Debugf != nil {
		keys := make([]string, 0, len(records))
		for _, r := range records {
			keys = append(keys, r.Key().String())
		}
		Debugf(ctx, "%s(keys=%+v)", operation, strings.Join(keys, ","))
	}
}

func logMultiKeys(ctx context.Context, operation string, keys []*dal.Key) {
	if Debugf != nil {
		s := make([]string, 0, len(keys))
		for _, k := range keys {
			s = append(s, k.String())
		}
		Debugf(ctx, "%s(keys=%+v)", operation, strings.Join(s, ","))
	}
}
