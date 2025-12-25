package dalgo2firestore

import (
	"context"

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
	return nil, dal.ErrNotImplementedYet
}
