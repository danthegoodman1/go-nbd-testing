# go-nbd-testing

Testing github.com/pojntfx/go-nbd and adding a factor server so backends can be dynamically created with their respective export name.

There's a hole in the current github.com/pojntfx/go-nbd/pkg/server in that there's no way for it to pass the export name to the Backend.

This implements a server that uses a backend factory function to allow attaching the export name to the backend, effectively creating a NamedBackend
