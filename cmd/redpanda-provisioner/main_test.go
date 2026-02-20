package main

import (
	"context"
	"log/slog"
	"testing"
)

func TestEnvOrDefault_EnvSet(t *testing.T) {
	t.Setenv("TEST_KEY", "custom")
	got := envOrDefault("TEST_KEY", "default")
	if got != "custom" {
		t.Errorf("expected 'custom', got %q", got)
	}
}

func TestEnvOrDefault_EnvEmpty(t *testing.T) {
	t.Setenv("TEST_KEY", "")
	got := envOrDefault("TEST_KEY", "default")
	if got != "default" {
		t.Errorf("expected 'default' for empty env, got %q", got)
	}
}

func TestEnvOrDefault_EnvUnset(t *testing.T) {
	got := envOrDefault("DEFINITELY_NOT_SET_12345", "fallback")
	if got != "fallback" {
		t.Errorf("expected 'fallback', got %q", got)
	}
}

func TestSetupLogging_Debug(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	setupLogging()
	if !slog.Default().Enabled(context.TODO(), slog.LevelDebug) {
		t.Error("expected debug level to be enabled")
	}
}

func TestSetupLogging_Warn(t *testing.T) {
	t.Setenv("LOG_LEVEL", "warn")
	setupLogging()
	if slog.Default().Enabled(context.TODO(), slog.LevelInfo) {
		t.Error("expected info level to be disabled at warn level")
	}
	if !slog.Default().Enabled(context.TODO(), slog.LevelWarn) {
		t.Error("expected warn level to be enabled")
	}
}

func TestSetupLogging_Error(t *testing.T) {
	t.Setenv("LOG_LEVEL", "error")
	setupLogging()
	if slog.Default().Enabled(context.TODO(), slog.LevelWarn) {
		t.Error("expected warn level to be disabled at error level")
	}
	if !slog.Default().Enabled(context.TODO(), slog.LevelError) {
		t.Error("expected error level to be enabled")
	}
}

func TestSetupLogging_DefaultInfo(t *testing.T) {
	t.Setenv("LOG_LEVEL", "info")
	setupLogging()
	if !slog.Default().Enabled(context.TODO(), slog.LevelInfo) {
		t.Error("expected info level to be enabled")
	}
	if slog.Default().Enabled(context.TODO(), slog.LevelDebug) {
		t.Error("expected debug level to be disabled at info level")
	}
}
