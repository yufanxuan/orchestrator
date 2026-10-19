package proxyapp

import (
	"log/slog"
	"os"

	"github.com/coreos/go-systemd/v22/daemon"
)

// notifySystemdReady reports master readiness to the service manager with
// sd_notify READY=1, for units running as Type=notify. Without NOTIFY_SOCKET
// (manual runs, containers, Type=simple/exec units) it is a no-op; the
// environment is left intact so a hot-upgrade successor forked from this
// process can report its own readiness. Only the master reports: workers
// never run RunMaster.
func notifySystemdReady(logger *slog.Logger) {
	if os.Getenv("NOTIFY_SOCKET") == "" {
		return
	}
	sent, err := daemon.SdNotify(false, daemon.SdNotifyReady)
	if err != nil || !sent {
		logger.Warn("proxy master systemd readiness notify", "sent", sent, "err", err)
	}
}
