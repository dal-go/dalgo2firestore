package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/iterator"
)

var _ dal.Reader = (*firestoreReader)(nil)

type firestoreReader struct {
	i           int // iteration
	query       dal.Query
	docIterator *firestore.DocumentIterator
}

func (d *firestoreReader) Next() (record dal.Record, err error) {
	if limit := d.query.Limit(); limit > 0 && d.i >= limit {
		return nil, dal.ErrNoMoreRecords
	}
	from := d.query.From()
	if into := d.query.Into(); into == nil {
		record = dal.NewRecordWithIncompleteKey(from.Name, d.query.IDKind(), nil)
	} else {
		record = into()
	}
	var doc *firestore.DocumentSnapshot
	if doc, err = d.docIterator.Next(); err != nil {
		if errors.Is(err, iterator.Done) {
			err = fmt.Errorf("%w: %v", dal.ErrNoMoreRecords, err)
		}
		return record, err
	}
	k := dal.NewKeyWithID(from.Name, doc.Ref.ID)
	data := record.Data()
	if rd, ok := data.(dal.RecordData); ok {
		data = rd.DTO()
	}
	if err = doc.DataTo(data); err != nil {
		return record, fmt.Errorf("failed to convert firestore document snapshot to %T: %w", data, err)
	}
	record = dal.NewRecordWithData(k, record.Data())
	d.i++
	return record, err
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
