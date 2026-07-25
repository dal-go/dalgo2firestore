package end2end

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
	"github.com/dal-go/dalgo2firestore"
)

// TestConformance runs the shared dalgotest suite against a live Firestore
// emulator, reusing the same emulator-lifecycle helpers as TestEndToEnd.
//
// This is the regression test for the finding that started this file: dalgo2fs
// validated on Insert but not on Set or Update/UpdateRecord (an adapter
// validating one write path and not another is exactly the drift the shared
// suite exists to catch). Insert's inline validation is gone now — the
// framework write pipeline behind dalgo2firestore.NewDatabase (dal.NewDB)
// enforces ValidatableRecord uniformly for every write, so this suite is the
// proof, not just a claim.
func TestConformance(t *testing.T) {
	log.Println("TestConformance() started...")
	cmd, cmdStdout, cmdStdErr := startFirebaseEmulators(t)
	defer func() {
		terminateFirebaseEmulators(t, cmd)
		cmd = nil
	}()
	emulatorExited := false
	go handleCommandStderr(t, cmdStdErr, &emulatorExited)
	select {
	case <-handleEmulatorClosing(t, cmd):
		emulatorExited = true
	case <-waitForEmulatorReadiness(t, cmdStdout, &emulatorExited):
		runConformanceAgainstEmulator(t)
	}
	time.Sleep(10 * time.Millisecond)
}

func runConformanceAgainstEmulator(t *testing.T) {
	if err := os.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:8080"); err != nil {
		t.Fatalf("failed to set env variable FIRESTORE_EMULATOR_HOST: %v", err)
	}
	firestoreProjectID := os.Getenv("FIREBASE_PROJECT_ID")
	if firestoreProjectID == "" {
		firestoreProjectID = "dalgo"
		_ = os.Setenv("FIREBASE_PROJECT_ID", firestoreProjectID)
	}

	ctx := context.Background()
	client, err := firestore.NewClient(ctx, firestoreProjectID)
	if err != nil {
		t.Fatalf("failed to create Firestore client: %v", err)
	}
	defer func() { _ = client.Close() }()

	dalgotest.RunConformance(t, func(t *testing.T) (dal.DB, func()) {
		return dalgo2firestore.NewDatabase("conformance-db", client), nil
	})
}
