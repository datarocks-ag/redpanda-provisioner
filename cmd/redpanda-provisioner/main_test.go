package main

import (
	"context"
	"log/slog"
	"reflect"
	"strings"
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

func TestResolveBrokerAddresses_YAMLWins(t *testing.T) {
	t.Setenv("REDPANDA_BROKERS", "env:9092")
	got := resolveBrokerAddresses([]string{"yaml-a:9092", "yaml-b:9092"})
	want := []string{"yaml-a:9092", "yaml-b:9092"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v (YAML should win when set)", got, want)
	}
}

func TestResolveBrokerAddresses_EnvFallback(t *testing.T) {
	t.Setenv("REDPANDA_BROKERS", "env-a:9092, env-b:9092")
	got := resolveBrokerAddresses(nil)
	want := []string{"env-a:9092", "env-b:9092"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v (env should fill in for empty YAML, with whitespace trimmed)", got, want)
	}
}

func TestResolveBrokerAddresses_DefaultWhenNothingSet(t *testing.T) {
	t.Setenv("REDPANDA_BROKERS", "")
	got := resolveBrokerAddresses(nil)
	want := []string{"localhost:9092"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestStringWithEnvFallback(t *testing.T) {
	t.Setenv("FALLBACK_KEY", "from-env")
	if got := stringWithEnvFallback("from-yaml", "FALLBACK_KEY"); got != "from-yaml" {
		t.Errorf("YAML should win when set, got %q", got)
	}
	if got := stringWithEnvFallback("", "FALLBACK_KEY"); got != "from-env" {
		t.Errorf("env fallback should fire when YAML empty, got %q", got)
	}
	t.Setenv("FALLBACK_KEY", "")
	if got := stringWithEnvFallback("", "FALLBACK_KEY"); got != "" {
		t.Errorf("expected empty string when both empty, got %q", got)
	}
}

func TestResolveTLSEnabled(t *testing.T) {
	yamlTrue := true
	yamlFalse := false

	t.Setenv("REDPANDA_TLS_ENABLED", "")
	if v, _ := resolveTLSEnabled(&yamlTrue); !v {
		t.Error("YAML true should win")
	}
	if v, _ := resolveTLSEnabled(nil); v {
		t.Error("expected false when both unset")
	}

	t.Setenv("REDPANDA_TLS_ENABLED", "true")
	if v, _ := resolveTLSEnabled(nil); !v {
		t.Error("expected env true when YAML is unset")
	}

	t.Setenv("REDPANDA_TLS_ENABLED", "1")
	if v, _ := resolveTLSEnabled(nil); !v {
		t.Error(`expected "1" to parse as true`)
	}

	// YAML-wins-when-set: an explicit YAML false must override env true.
	// Without the *bool tri-state this case used to silently downgrade to
	// env-wins because YAML zero-value (false) was indistinguishable from
	// "not set".
	t.Setenv("REDPANDA_TLS_ENABLED", "true")
	if v, _ := resolveTLSEnabled(&yamlFalse); v {
		t.Error("YAML false should override env true")
	}

	t.Setenv("REDPANDA_TLS_ENABLED", "definitely-not-a-bool")
	if _, err := resolveTLSEnabled(nil); err == nil {
		t.Error("expected error for non-bool env value")
	}
	// An invalid env value is ignored when YAML wins outright.
	if v, err := resolveTLSEnabled(&yamlFalse); err != nil || v {
		t.Errorf("YAML false with garbage env: got v=%v err=%v, want false/nil", v, err)
	}
}

func TestValidateMergedBrokerSASL(t *testing.T) {
	cases := []struct {
		name                       string
		user, pass, mech           string
		wantErr                    bool
		errSubstr                  string
	}{
		{name: "all-empty (no SASL)", wantErr: false},
		{name: "full pair SCRAM-256", user: "u", pass: "p", mech: "SCRAM-SHA-256", wantErr: false},
		{name: "full pair SCRAM-512", user: "u", pass: "p", mech: "SCRAM-SHA-512", wantErr: false},
		{name: "username only", user: "u", pass: "", mech: "SCRAM-SHA-256", wantErr: true, errSubstr: "both be set"},
		{name: "password only", user: "", pass: "p", mech: "SCRAM-SHA-256", wantErr: true, errSubstr: "both be set"},
		{name: "PLAIN rejected", user: "u", pass: "p", mech: "PLAIN", wantErr: true, errSubstr: "unsupported"},
		{name: "empty mechanism with credentials", user: "u", pass: "p", mech: "", wantErr: true, errSubstr: "unsupported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMergedBrokerSASL(tc.user, tc.pass, tc.mech)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got err=%v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr && tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("expected error containing %q, got: %v", tc.errSubstr, err)
			}
		})
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
