package dalgo2firestore

import (
	"context"
	"fmt"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

var _ dal.QueryExecutor = (*queryExecutor)(nil)

type queryExecutor struct {
	getRecordsReader func(c context.Context, query dal.Query) (reader dal.RecordsReader, err error)
}

func (q queryExecutor) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return q.getRecordsReader(ctx, query)
}

func (q queryExecutor) ExecuteQueryToRecordsetReader(_ context.Context, _ dal.Query, _ ...recordset.Option) (dal.RecordsetReader, error) {
	// dal.ErrNotSupported is the dalgo capability-reporting contract:
	// callers (e.g. the dalgo end2end suite) detect it via errors.Is and skip.
	return nil, fmt.Errorf("%w: recordset reader is not implemented by dalgo2firestore", dal.ErrNotSupported)
}
