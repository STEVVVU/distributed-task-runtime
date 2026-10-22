//go:build !benchmark

package main

func startBenchmarkProfile() (func(), error) {
	return func() {}, nil
}
