package observability_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/observability"
)

func TestZapRollingFileLoggerWritesLocalFile(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "stellflow.log")
	logger, err := observability.NewZapRollingFileLogger(observability.ZapRollingFileConfig{
		Filename:  logFile,
		Level:     "debug",
		MaxSizeMB: 1,
	})
	if err != nil {
		t.Fatalf("NewZapRollingFileLogger() error = %v", err)
	}
	defer logger.Close()
	logger.Info(context.Background(), "hello stellflow", observability.String("component", "test"))
	if err := logger.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "hello stellflow") || !strings.Contains(content, "component") {
		t.Fatalf("log content = %q", content)
	}
}
