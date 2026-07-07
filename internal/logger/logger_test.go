package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestLogger_JSON(t *testing.T) {
	var buf bytes.Buffer
	l, err := New("info", "json", &buf)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	l.Info("hello world", "key", "val")
	output := buf.String()

	if !strings.Contains(output, `"level":"INFO"`) {
		t.Errorf("expected output to contain level INFO, got: %q", output)
	}
	if !strings.Contains(output, `"msg":"hello world"`) {
		t.Errorf("expected output to contain msg, got: %q", output)
	}
	if !strings.Contains(output, `"key":"val"`) {
		t.Errorf("expected output to contain key/val, got: %q", output)
	}
}

func TestLogger_Text(t *testing.T) {
	var buf bytes.Buffer
	l, err := New("debug", "text", &buf)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	l.Debug("debug message", "foo", "bar")
	output := buf.String()

	if !strings.Contains(output, "level=DEBUG") {
		t.Errorf("expected output to contain level=DEBUG, got: %q", output)
	}
	if !strings.Contains(output, "msg=\"debug message\"") && !strings.Contains(output, "msg=debug message") {
		t.Errorf("expected output to contain debug message, got: %q", output)
	}
}

func TestLogger_Filtering(t *testing.T) {
	var buf bytes.Buffer
	l, err := New("warn", "json", &buf)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	l.Info("should be ignored")
	if buf.Len() > 0 {
		t.Errorf("expected Info log to be ignored, got: %q", buf.String())
	}

	l.Warn("should be recorded")
	if buf.Len() == 0 {
		t.Error("expected Warn log to be recorded")
	}
}

func TestLogger_Errors(t *testing.T) {
	_, err := New("invalid_level", "json", nil)
	if err == nil {
		t.Error("expected error for invalid level, got nil")
	}

	_, err = New("info", "invalid_format", nil)
	if err == nil {
		t.Error("expected error for invalid format, got nil")
	}
}
