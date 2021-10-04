package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/pkg/errors"
	"github.com/strongo/dalgo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type getter struct {
	doc    func(key *dalgo.Key) *firestore.DocumentRef
	dataTo func(ds *firestore.DocumentSnapshot, p interface{}) error
	get    func(ctx context.Context, docRef *firestore.DocumentRef) (_ *firestore.DocumentSnapshot, err error)
	getAll func(ctx context.Context, docRefs []*firestore.DocumentRef) (_ []*firestore.DocumentSnapshot, err error)
}

func newGetter(dtb database) getter {
	return getter{
		doc:    dtb.doc,
		get:    get,
		getAll: dtb.client.GetAll,
		dataTo: func(ds *firestore.DocumentSnapshot, p interface{}) error {
			return ds.DataTo(p)
		},
	}
}

func (g getter) Get(ctx context.Context, record dalgo.Record) error {
	key := record.Key()
	docRef := g.doc(key)
	docSnapshot, err := g.get(ctx, docRef)
	return docSnapshotToRecord(err, docSnapshot, record, g.dataTo)
}

func docSnapshotToRecord(
	err error,
	docSnapshot *firestore.DocumentSnapshot,
	record dalgo.Record,
	dataTo func(ds *firestore.DocumentSnapshot, p interface{}) error,
) error {
	if err != nil {
		if status.Code(err) == codes.NotFound {
			err = dalgo.NewErrNotFoundByKey(record.Key(), err)
		}
		record.SetError(err)
		return err
	}
	recData := record.Data()
	err = dataTo(docSnapshot, recData)
	if status.Code(err) == codes.NotFound {
		err = dalgo.NewErrNotFoundByKey(record.Key(), err)
		record.SetError(err)
		return err
	}
	if err != nil {
		err = errors.Wrapf(err, "failed to marshal record data into a target of type %T", recData)
	}
	record.SetError(err)
	return nil
}

func (g getter) GetMulti(ctx context.Context, records []dalgo.Record) error {
	docRefs := make([]*firestore.DocumentRef, len(records))
	for i, rec := range records {
		docRefs[i] = g.doc(rec.Key())
	}
	docSnapshots, err := g.getAll(ctx, docRefs)
	if err != nil {
		return err
	}
	allErrors := make([]error, 0, len(records))
	for i, rec := range records {
		if err = docSnapshotToRecord(nil, docSnapshots[i], rec, g.dataTo); err != nil && !dalgo.IsNotFound(err) {
			allErrors = append(allErrors, err)
		}
	}
	if len(allErrors) > 0 {
		return errors.Wrapf(allErrors[0], "failed to marshal data for %v records out of %v", len(allErrors), len(records))
	}
	return nil
}
