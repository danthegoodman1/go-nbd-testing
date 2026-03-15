package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	nbdbackend "github.com/pojntfx/go-nbd/pkg/backend"
)

const libnbdClientImage = "go-nbd-testing/libnbd-client:latest"

type linuxClientResult struct {
	exportName string
	stdout     string
	err        error
}

func TestHandleWithLibnbdLinuxClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed Linux integration test in short mode")
	}

	if os.Getenv("NBD_REAL_LINUX_E2E") != "1" {
		t.Skip("set NBD_REAL_LINUX_E2E=1 to run the Docker-backed Linux integration test")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := buildLibnbdClientImage(ctx); err != nil {
		t.Fatalf("build libnbd client image: %v", err)
	}

	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	var (
		backendsMu sync.Mutex
		backends   = map[string]*MemoryBackend{}
		handlers   sync.WaitGroup
		serverErrs = make(chan error, 8)
		stopServer = make(chan struct{})
	)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-stopServer:
					return
				default:
					serverErrs <- err
					return
				}
			}

			handlers.Add(1)
			go func(conn net.Conn) {
				defer handlers.Done()
				defer conn.Close()

				err := Handle(conn, func(info ConnInfo) (nbdbackend.Backend, error) {
					backend := NewMemoryBackend(info.ExportName, 64)

					backendsMu.Lock()
					backends[info.ExportName] = backend
					backendsMu.Unlock()

					return backend, nil
				}, &Options{
					ExportDescription: "linux client test export",
					SupportsMultiConn: true,
				})
				if err != nil && !errors.Is(err, net.ErrClosed) {
					serverErrs <- err
				}
			}(conn)
		}
	}()

	port := listener.Addr().(*net.TCPAddr).Port

	results := make(chan linuxClientResult, 2)
	runClient := func(exportName string, payload string) {
		stdout, err := runLibnbdLinuxClient(ctx, port, exportName, payload)
		results <- linuxClientResult{
			exportName: exportName,
			stdout:     stdout,
			err:        err,
		}
	}

	go runClient("tenant-a", "AAAA")
	go runClient("tenant-b", "BBBB")

	resultA := <-results
	resultB := <-results

	close(stopServer)
	_ = listener.Close()
	handlers.Wait()

	resultsByExport := map[string]linuxClientResult{
		resultA.exportName: resultA,
		resultB.exportName: resultB,
	}

	for _, result := range []linuxClientResult{resultA, resultB} {
		if result.err != nil {
			t.Fatalf("linux client %s: %v", result.exportName, result.err)
		}
	}

	assertLinuxClientResult(t, resultsByExport["tenant-a"], "AAAA")
	assertLinuxClientResult(t, resultsByExport["tenant-b"], "BBBB")

	backendsMu.Lock()
	backendA := backends["tenant-a"]
	backendB := backends["tenant-b"]
	backendsMu.Unlock()

	if backendA == nil || backendB == nil {
		t.Fatalf("expected backends for both exports, got %v", mapsKeys(backends))
	}

	for _, backend := range []*MemoryBackend{backendA, backendB} {
		operations := backend.Operations()
		if !hasOperation(operations, "write", 8, 4) {
			t.Fatalf("expected write operation for %s, got %#v", backend.ExportName, operations)
		}

		if !hasOperation(operations, "read", 8, 4) {
			t.Fatalf("expected read operation for %s, got %#v", backend.ExportName, operations)
		}

		if !hasOperation(operations, "sync", 0, 0) {
			t.Fatalf("expected sync operation for %s, got %#v", backend.ExportName, operations)
		}
	}

	select {
	case err := <-serverErrs:
		t.Fatalf("server error: %v", err)
	default:
	}
}

func buildLibnbdClientImage(ctx context.Context) error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("error in os.Getwd: %w", err)
	}

	dockerfilePath := filepath.Join(wd, "testdata", "libnbd-client.Dockerfile")
	contextDir := filepath.Join(wd, "testdata")

	cmd := exec.CommandContext(ctx, "docker", "build", "-f", dockerfilePath, "-t", libnbdClientImage, contextDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error in docker build: %w\n%s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

func runLibnbdLinuxClient(ctx context.Context, port int, exportName string, payload string) (string, error) {
	uri := fmt.Sprintf("nbd://%s:%d/%s", dockerHostName(), port, exportName)
	script := `set -euo pipefail
nbdinfo "$NBD_URI" >/dev/null 2>&1
nbdsh -u "$NBD_URI" -c "import os, time; payload=os.environ['PAYLOAD'].encode(); time.sleep(1); h.pwrite(payload, 8); time.sleep(1); print('RESULT=' + h.pread(len(payload), 8).decode(), end=''); h.shutdown()"`

	args := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		args = append(args, "--add-host", "host.docker.internal:host-gateway")
	}

	args = append(args,
		"-e", "NBD_URI="+uri,
		"-e", "PAYLOAD="+payload,
		libnbdClientImage,
		"bash", "-lc", script,
	)

	cmd := exec.CommandContext(ctx, "docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("error in docker run: %w", err)
	}

	return string(output), nil
}

func dockerHostName() string {
	return "host.docker.internal"
}

func assertLinuxClientResult(t *testing.T, result linuxClientResult, payload string) {
	t.Helper()

	if !strings.Contains(result.stdout, "RESULT="+payload) {
		t.Fatalf("linux client %s returned %q, want RESULT=%s", result.exportName, result.stdout, payload)
	}
}
