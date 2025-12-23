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

func (q queryExecutor) GetRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return q.getRecordsReader(ctx, query)
}

func (q queryExecutor) GetRecordsetReader(_ context.Context, _ dal.Query, _ *recordset.Recordset) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotImplementedYet
}
