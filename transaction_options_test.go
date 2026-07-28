package dalgo2firestore

import (
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestCreateFirestoreTransactionOptions(t *testing.T) {
	for _, tc := range []struct {
		name                string
		options             []dal.TransactionOption
		wantReadonly        bool
		wantAttemptsPresent bool
		wantAttempts        int
	}{
		{
			name: "uses Firestore defaults when DALgo options are unset",
		},
		{
			name:                "maps readonly and positive attempt count",
			options:             []dal.TransactionOption{dal.TxWithReadonly(), dal.TxWithAttempts(3)},
			wantReadonly:        true,
			wantAttemptsPresent: true,
			wantAttempts:        3,
		},
		{
			name:    "does not override Firestore defaults with zero attempts",
			options: []dal.TransactionOption{dal.TxWithAttempts(0)},
		},
		{
			name:    "does not pass an invalid negative attempt count",
			options: []dal.TransactionOption{dal.TxWithAttempts(-1)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := createFirestoreTransactionOptions(tc.options)
			var gotReadonly bool
			var gotAttemptsPresent bool
			var gotAttempts int
			for _, option := range options {
				switch reflect.TypeOf(option).String() {
				case "firestore.ro":
					gotReadonly = true
				case "firestore.maxAttempts":
					gotAttemptsPresent = true
					gotAttempts = int(reflect.ValueOf(option).Int())
				default:
					t.Fatalf("unexpected Firestore transaction option type %T", option)
				}
			}
			if gotReadonly != tc.wantReadonly {
				t.Errorf("readonly option = %v, want %v", gotReadonly, tc.wantReadonly)
			}
			if gotAttemptsPresent != tc.wantAttemptsPresent {
				t.Errorf("maximum-attempts option present = %v, want %v", gotAttemptsPresent, tc.wantAttemptsPresent)
			}
			if gotAttempts != tc.wantAttempts {
				t.Errorf("maximum attempts = %d, want %d", gotAttempts, tc.wantAttempts)
			}
		})
	}
}
