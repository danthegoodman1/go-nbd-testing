# go-nbd-testing

This repo implements a small replacement for `github.com/pojntfx/go-nbd/pkg/server` that opens backends through a factory instead of requiring a prebuilt backend up front.

The goal is multitenancy: the factory receives connection info, including the requested export name, so each client can get a different backend for the same server instance.

Tests:

```sh
go test ./...
```

Real Linux client integration test:

```sh
NBD_REAL_LINUX_E2E=1 go test -run TestHandleWithLibnbdLinuxClients -v ./server
```
