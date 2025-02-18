package dalgo2firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/api/iterator"
	"reflect"
	"strconv"
)

var _ dal.Reader = (*firestoreReader)(nil)

type firestoreReader struct {
	i           int // iteration
	query       dal.Query
	docIterator *firestore.DocumentIterator
}

func (d *firestoreReader) Close() error {
	return nil
}

func (d *firestoreReader) Next() (record dal.Record, err error) {
	if limit := d.query.Limit(); limit > 0 && d.i >= limit {
		return nil, dal.ErrNoMoreRecords
	}
	if into := d.query.Into(); into == nil {
		from := d.query.From()
		record = dal.NewRecordWithIncompleteKey(from.Name(), d.query.IDKind(), nil)
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
	record.SetError(nil)
	data := record.Data()
	rd, isDataWrapper := data.(dal.DataWrapper)
	if isDataWrapper {
		if data = rd.Data(); data == nil {
			return record, fmt.Errorf("DataWrapper.Data() returned nil")
		}
	}
	if data != nil {
		if err = doc.DataTo(data); err != nil {
			return record, fmt.Errorf("failed to convert firestore document snapshot to %T: %w", data, err)
		}
	}
	k := record.Key()
	k.ID, err = idFromFirestoreDocRef(doc.Ref, k.IDKind)
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
			return int(id), nil
		case reflect.Int32:
			return int(id), nil
		case reflect.Int16:
			return int(id), nil
		case reflect.Int8:
			return int(id), nil
		default:
			return key, fmt.Errorf("unsupported id type: %T=%v", idKind, idKind)
		}
	}
}
