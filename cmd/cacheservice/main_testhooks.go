//go:build cacheservice_testhooks

package main

// Test hooks for the built binary's tests (main_stop_test.go), which build it
// with -tags cacheservice_testhooks. A release build has none of this.

import (
	"log/slog"
	"os"

	"github.com/dagger/dagger/cmd/cacheservice/server"
)

func init() {
	testHookStop = func(svc *server.Service, logger *slog.Logger) {
		if os.Getenv(stopTestHoldEnv) == "1" {
			logger.Warn("test hook: the stop waits for work that never ends (" + stopTestHoldEnv + ")")
			svc.HoldWaitForTest()
		}
	}
}
