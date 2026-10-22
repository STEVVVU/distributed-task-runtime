//go:build !benchmark

package main

import "testing"

func TestNormalBuildDoesNotEnableProfileEndpoint(t *testing.T) {
	t.Setenv("TASK_RUNTIME_PROFILE_ADDR", "0.0.0.0:9000")
	stop, err := startBenchmarkProfile()
	if err != nil {
		t.Fatal(err)
	}
	stop()
}
