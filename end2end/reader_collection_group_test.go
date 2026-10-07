package end2end

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2firestore"
)

func TestFirestoreReaderCollectionGroupUsesReturnedDocumentPaths(t *testing.T) {
	cmd, cmdStdout, cmdStdErr := startFirebaseEmulators(t)
	defer terminateFirebaseEmulators(t, cmd)

	emulatorExited := false
	go handleCommandStderr(t, cmdStdErr, &emulatorExited)
	select {
	case <-handleEmulatorClosing(t, cmd):
		emulatorExited = true
		t.Fatal("Firestore emulator exited before becoming ready")
	case <-waitForEmulatorReadiness(t, cmdStdout, &emulatorExited):
		runCollectionGroupPathQuery(t)
	}
	time.Sleep(10 * time.Millisecond)
}

func runCollectionGroupPathQuery(t *testing.T) {
	t.Helper()
	if err := os.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:8080"); err != nil {
		t.Fatalf("set Firestore emulator host: %v", err)
	}

	ctx := context.Background()
	projectID := os.Getenv("FIREBASE_PROJECT_ID")
	if projectID == "" {
		projectID = "dalgo"
	}
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("create Firestore client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Firestore client: %v", err)
		}
	}()

	unique := fmt.Sprintf("reader-path-%d", time.Now().UnixNano())
	paths := []string{
		"spaces/" + unique + "-a/projects/project-a/queries/query-a",
		"spaces/" + unique + "-b/projects/project-b/queries/query-b",
		"spaces/" + unique + "-c/projects/deleted-project/archives/archive/queries/query-c",
	}
	for _, path := range paths {
		ref := client.Doc(path)
		if _, err := ref.Set(ctx, map[string]any{"fixture": true}); err != nil {
			t.Fatalf("seed collection-group document %q: %v", path, err)
		}
		t.Cleanup(func() {
			if _, err := ref.Delete(ctx); err != nil {
				t.Errorf("delete fixture document %q: %v", path, err)
			}
		})
	}

	// Firestore preserves subcollection documents when an ancestor is deleted.
	deletedProject := client.Doc("spaces/" + unique + "-c/projects/deleted-project")
	if _, err := deletedProject.Set(ctx, map[string]any{"fixture": true}); err != nil {
		t.Fatalf("seed parent for orphaned subcollection: %v", err)
	}
	if _, err := deletedProject.Delete(ctx); err != nil {
		t.Fatalf("delete project ancestor: %v", err)
	}

	query := dal.NewQueryBuilder(dal.From(dal.NewCollectionGroupRef("queries", ""))).SelectKeysOnly(reflect.String)
	reader, err := dalgo2firestore.NewDatabase("collection-group-path-test", client).ExecuteQueryToRecordsReader(ctx, query)
	if err != nil {
		t.Fatalf("execute collection-group query: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close records reader: %v", err)
		}
	}()

	want := map[string]bool{}
	for _, path := range paths {
		want[path] = false
	}
	for {
		record, err := reader.Next()
		if errors.Is(err, dal.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			t.Fatalf("read collection-group result: %v", err)
		}
		path := record.Key().String()
		seen, expected := want[path]
		if !expected {
			t.Fatalf("unexpected collection-group result path %q", path)
		}
		if seen {
			t.Fatalf("duplicate collection-group result path %q", path)
		}
		want[path] = true
	}
	for path, seen := range want {
		if !seen {
			t.Errorf("collection-group query did not return %q", path)
		}
	}
}
