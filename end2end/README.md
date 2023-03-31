# End-to-end tests of dalgo

End-to-end tests are using [Firebase Local Emulator Suite](https://firebase.google.com/docs/emulator-suite).

## Source code

End-to-end tests are initialized at [e2e_test.go](e2e_test.go).

## Known issue

Sometimes Firebase emulator is not shut down correctly and process(es) holding ports need to be killed:

    sudo kill -9 $(lsof -i :8080 -t)

