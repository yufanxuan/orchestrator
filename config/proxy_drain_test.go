package config

import (
	"strings"
	"testing"
	"time"
)

func TestProxyDrainTimeout(t *testing.T) {
	decode := func(body string) (*Proxy, error) {
		return DecodeProxy(strings.NewReader(body))
	}
	withDefault, err := decode("paths: { run_root: /run/sandbox }\nconfig_socket: /tmp/config.sock\n")
	if err != nil {
		t.Fatal(err)
	}
	if withDefault.DrainTimeout != "30m" || withDefault.DrainTimeoutDur() != 30*time.Minute {
		t.Fatalf("default drain_timeout=%q dur=%v, want 30m", withDefault.DrainTimeout, withDefault.DrainTimeoutDur())
	}
	configured, err := decode("paths: { run_root: /run/sandbox }\nconfig_socket: /tmp/config.sock\ndrain_timeout: 5s\n")
	if err != nil {
		t.Fatal(err)
	}
	if configured.DrainTimeoutDur() != 5*time.Second {
		t.Fatalf("drain_timeout dur=%v, want 5s", configured.DrainTimeoutDur())
	}
	if _, err := decode("paths: { run_root: /run/sandbox }\nconfig_socket: /tmp/config.sock\ndrain_timeout: bogus\n"); err == nil {
		t.Fatal("invalid drain_timeout accepted")
	}
}
