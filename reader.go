package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/dal-go/dalgo/dal"
)

var _ dal.Reader = (*firestoreReader)(nil)

type firestoreReader struct {
	docIterator *firestore.DocumentIterator
}

func (f *firestoreReader) Next() (dal.Record, error) {
	//TODO implement me
	panic("implement me")
}

func (f *firestoreReader) Cursor() (string, error) {
	//TODO implement me
	panic("implement me")
}

func newFirestoreReader(c context.Context, client *firestore.Client, query dal.Query) (reader dal.Reader, err error) {
	r := new(firestoreReader)
	r.docIterator, err = dalQuery2firestoreIterator(c, query, client)
	return reader, err
}
