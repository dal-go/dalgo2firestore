package end2end

import (
	"bytes"
	"cloud.google.com/go/firestore"
	"context"
	end2end "github.com/dal-go/dalgo-end2end-tests"
	"github.com/dal-go/dalgo2firestore"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestEndToEnd(t *testing.T) {
	log.Println("TestEndToEnd() started...")
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
		testEndToEnd(t)
	}
	time.Sleep(10 * time.Millisecond)
}

func handleCommandStderr(t *testing.T, stderr *bytes.Buffer, emulatorExited *bool) {
	reading := false
	for {
		if *emulatorExited {
			return
		}
		line, err := stderr.ReadString('\n')
		if err == io.EOF {
			reading = false
			time.Sleep(9 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Errorf("Failed to read from Firebase emulator STDERR: %v", err)
			return
		}
		if line = strings.TrimSpace(line); line == "" {
			continue
		}
		if !reading {
			reading = true
			t.Error("ERROR in Firebase emulator:")
		}
		t.Error("\t" + line)
	}
}

func terminateFirebaseEmulators(t *testing.T, cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	// TODO(help-wanted): Consider cmd.Cancel() ?
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			t.Error("Failed to terminate Firebase emulator:", err)
			return
		}
	}
	t.Log("Firebase simulator terminated")
}

func startFirebaseEmulators(t *testing.T) (cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
	cmd = exec.Command("firebase",
		"emulators:start",
		"-c", "./firebase/firebase.json",
		"--only", "firestore",
		"--project", "dalgo",
	)

	stdout = new(bytes.Buffer)
	stderr = new(bytes.Buffer)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	t.Log("Starting Firebase emulator...")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start Firebase emulator: %v", err)
	}
	return
}

func waitForEmulatorReadiness(t *testing.T, cmdOutput *bytes.Buffer, emulatorExited *bool) (emulatorsReady chan bool) {
	emulatorsReady = make(chan bool)
	//time.Sleep(3 * time.Second)
	go func() {
		t.Log("Awaiting for Firebase emulator to be ready...")
		for i := 1; true; i++ {
			if *emulatorExited {
				return
			}
			line, err := cmdOutput.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				t.Errorf("Failed to read: %v", err)
				return
			}
			if strings.Contains(line, "All emulators ready!") {
				//t.Log("Firebase emulators are ready.")
				emulatorsReady <- true
				//close(emulatorsReady)
			}
		}
	}()
	return
}

func handleEmulatorClosing(t *testing.T, cmd *exec.Cmd) (emulatorErrors chan error) {
	emulatorErrors = make(chan error)
	go func() {
		if err := cmd.Wait(); err != nil {
			if err.Error() == "signal: killed" {
				t.Log("Firebase emulator killed.")
			} else {
				t.Error("Firebase emulator failed:", err)
				emulatorErrors <- err
			}
		} else {
			t.Log("Firebase emulator completed.")
		}
		close(emulatorErrors)
	}()
	return
}

func testEndToEnd(t *testing.T) {
	if err := os.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:8080"); err != nil {
		t.Fatalf("Failed to set env variable FIRESTORE_EMULATOR_HOST: %v", err)
	}
	firestoreProjectID := os.Getenv("FIREBASE_PROJECT_ID")

	if firestoreProjectID == "" {
		firestoreProjectID = "dalgo"
		//t.Fatalf("Environment variable FIREBASE_PROJECT_ID is not set")
	}
	log.Println("Firestore Project ID:", firestoreProjectID)
	//log.Println("ENV: GOOGLE_APPLICATION_CREDENTIALS:", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"))

	ctx := context.Background()

	//var client *firestore.Client
	client, err := firestore.NewClient(ctx, firestoreProjectID)
	if err != nil {
		t.Fatalf("failed to create Firestore client: %v", err)
	}
	db := dalgo2firestore.NewDatabase("test-db", client)

	end2end.TestDalgoDB(t, db, nil, false)
}
