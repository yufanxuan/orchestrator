package proxyadmission

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"testing"
)

const attachHelperEnv = "KUASAR_TEST_ADMISSION_ATTACH_HELPER"

func init() {
	if os.Getenv(attachHelperEnv) == "1" {
		runAttachHelper()
	}
}

func runAttachHelper() {
	fd, err := strconv.Atoi(os.Getenv("KUASAR_TEST_ADMISSION_FD"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad fd:", err)
		os.Exit(2)
	}
	size, err := strconv.Atoi(os.Getenv("KUASAR_TEST_ADMISSION_SIZE"))
	if err != nil || size < 256 {
		fmt.Fprintln(os.Stderr, "bad size")
		os.Exit(2)
	}
	want := os.Getenv("KUASAR_TEST_ADMISSION_DIGEST")
	data, err := syscall.Mmap(fd, 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mmap:", err)
		os.Exit(2)
	}
	fmt.Printf("CHILD-PREFIX %x\n", data[:32])
	got := hex.EncodeToString(sha256Sum256(data[:256]))
	if got != want {
		fmt.Fprintln(os.Stderr, "ATTACH-DIGEST-MISMATCH got", got, "want", want)
		os.Exit(2)
	}
	nonZero := false
	for _, b := range data[:256] {
		if b != 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		fmt.Fprintln(os.Stderr, "ATTACH-EMPTY")
		os.Exit(2)
	}
	fmt.Println("ATTACH-OK")
	os.Exit(0)
}

func sha256Sum256(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

// TestArenaMemfdSurvivesExecAttach proves the precondition for the hot-upgrade
// admission handover: a memfd arena created by one process can be mmap'd with
// identical content by a fresh process after FD inheritance across exec.
func TestArenaMemfdSurvivesExecAttach(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	if runtime.GOARCH != "arm64" && runtime.GOARCH != "amd64" {
		t.Skip("unsupported arch")
	}
	const capacity, workers = 16, 2
	master, err := NewMaster(capacity, workers)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if err := master.BeginWorker(0, 1); err != nil {
		t.Fatal(err)
	}
	size, err := Size(capacity, workers)
	if err != nil {
		t.Fatal(err)
	}
	if len(master.mapping.data) < 256 {
		t.Fatalf("arena too small: %d", len(master.mapping.data))
	}
	digest := hex.EncodeToString(sha256Sum256(master.mapping.data[:256]))
	t.Logf("PARENT-PREFIX %x", master.mapping.data[:32])

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		attachHelperEnv+"=1",
		"KUASAR_TEST_ADMISSION_FD=3",
		"KUASAR_TEST_ADMISSION_SIZE="+strconv.Itoa(size),
		"KUASAR_TEST_ADMISSION_DIGEST="+digest,
	)
	cmd.ExtraFiles = []*os.File{master.file}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child attach: %v\n%s", err, out)
	}
	t.Logf("RESULT admission memfd_exec_attach=OK size=%d digest_prefix=%s (hot-upgrade AttachMaster precondition holds)",
		size, digest[:12])
}
