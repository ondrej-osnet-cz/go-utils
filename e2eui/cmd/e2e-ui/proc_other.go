//go:build !unix

package main

import "os/exec"

// killProcessGroup is a no-op here: cancelling kills only the go command and
// WaitDelay bounds the wait for its child.
func killProcessGroup(*exec.Cmd) {}
