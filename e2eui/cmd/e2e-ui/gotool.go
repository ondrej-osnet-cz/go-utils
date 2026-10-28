package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"time"
)

// pkgTests is one package with its top-level tests.
type pkgTests struct {
	Package string   `json:"package"`
	Tests   []string `json:"tests"`
}

// testEvent is one line of `go test -json` (see `go doc test2json`).
type testEvent struct {
	Action     string   `json:"Action"`
	Package    string   `json:"Package"`
	ImportPath string   `json:"ImportPath"` // set instead of Package on build events
	Test       string   `json:"Test"`
	Elapsed    *float64 `json:"Elapsed"`
	Output     string   `json:"Output"`
}

// goTool lists and runs tests with the go command in dir.
type goTool struct {
	dir string
}

// testName matches what `go test -list` prints for runnable tests.
var testName = regexp.MustCompile(`^(Test|Example|Fuzz)\S*$`)

func (g goTool) List(ctx context.Context, pkgs []string) ([]pkgTests, error) {
	cmd := exec.CommandContext(ctx, "go", append([]string{"test", "-list", ".", "-json"}, pkgs...)...) //nolint:gosec // G204: patterns come from the -pkg flag, no shell
	cmd.Dir = g.dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go test -list: %w: %s%s", err, stderr.Bytes(), listFailure(out))
	}
	return parseList(bytes.NewReader(out))
}

// parseList reads `go test -list . -json` output. Packages without tests are left out.
func parseList(r io.Reader) ([]pkgTests, error) {
	byPkg := map[string][]string{}
	dec := json.NewDecoder(r)
	for {
		var ev testEvent
		if err := dec.Decode(&ev); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode test list: %w", err)
		}
		if name := strings.TrimSpace(ev.Output); ev.Action == "output" && ev.Test == "" && testName.MatchString(name) {
			byPkg[ev.Package] = append(byPkg[ev.Package], name)
		}
	}
	list := make([]pkgTests, 0, len(byPkg))
	for pkg, tests := range byPkg {
		list = append(list, pkgTests{Package: pkg, Tests: tests})
	}
	slices.SortFunc(list, func(a, b pkgTests) int { return strings.Compare(a.Package, b.Package) })
	return list, nil
}

// listFailure extracts the output of a failed listing (e.g. a compile error,
// which `go test -json` reports on stdout).
func listFailure(out []byte) string {
	var b strings.Builder
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var ev testEvent
		if dec.Decode(&ev) != nil {
			return b.String()
		}
		if ev.Action == "output" || ev.Action == "build-output" {
			b.WriteString(ev.Output)
		}
	}
}

// Run runs `go test -json` for pkg with the -run pattern ("" runs all) and the
// extra environment, and reports every event. A failing test is not an error.
func (g goTool) Run(ctx context.Context, pkg, pattern string, env []string, report func(testEvent)) error {
	args := []string{"test", "-json", "-count=1"}
	if pattern != "" {
		args = append(args, "-run", pattern)
	}
	cmd := exec.CommandContext(ctx, "go", append(args, pkg)...) //nolint:gosec // G204: pkg is from the listing, the pattern is quoted, no shell
	cmd.Dir = g.dir
	cmd.Env = append(os.Environ(), env...)
	killProcessGroup(cmd) // the test binary is a child of go; stop it too
	cmd.WaitDelay = 5 * time.Second

	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start go test: %w", err)
	}
	waited := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		_ = pw.Close()
		waited <- err
	}()

	sc := bufio.NewScanner(pr)
	sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for sc.Scan() {
		var ev testEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil || ev.Action == "" {
			ev = testEvent{Action: "output", Output: sc.Text() + "\n"} // stderr of the go command
		}
		if ev.Package == "" {
			ev.Package = ev.ImportPath
		}
		report(ev)
	}
	scanErr := sc.Err()
	_, _ = io.Copy(io.Discard, pr) // after a scan error, let go test finish writing

	err := <-waited
	if scanErr != nil {
		return fmt.Errorf("read go test output: %w", scanErr)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("go test: %w", ctx.Err())
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return nil // tests failed or did not build; the events tell
	}
	if err != nil {
		return fmt.Errorf("go test: %w", err)
	}
	return nil
}

// runPattern builds the -run pattern that selects exactly tests (with their
// subtests); none selects all. One test may be a subtest, each level is then
// anchored and quoted. Several tests must be top-level ones.
func runPattern(tests []string) string {
	switch len(tests) {
	case 0:
		return ""
	case 1:
		parts := strings.Split(tests[0], "/")
		for i, p := range parts {
			parts[i] = "^" + regexp.QuoteMeta(p) + "$"
		}
		return strings.Join(parts, "/")
	}
	quoted := make([]string, len(tests))
	for i, t := range tests {
		quoted[i] = regexp.QuoteMeta(t)
	}
	return "^(?:" + strings.Join(quoted, "|") + ")$"
}
