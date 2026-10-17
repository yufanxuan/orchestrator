package proxyapp

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// Hot-upgrade descriptor environment. A successor master is forked by the old
// master (exec of the on-disk binary) with these variables set and the
// inherited listener descriptors passed as extra files. The environment is
// consumed by the first RestoreHotUpgradeListeners call and never reaches
// worker processes.
const (
	hotUpgradeEnv    = "KUASAR_PROXY_HOT_UPGRADE"
	hotUpgradeDataFD = "KUASAR_PROXY_HOT_UPGRADE_DATA_FD"
	hotUpgradeMMDSFD = "KUASAR_PROXY_HOT_UPGRADE_MMDS_FD"
)

// HotUpgradeActive reports whether this process was forked by a running proxy
// master for an in-place upgrade and must restore inherited listeners instead
// of binding.
func HotUpgradeActive() bool {
	return os.Getenv(hotUpgradeEnv) == "1"
}

// RestoreHotUpgradeListeners reconstructs the data listener and the optional
// MMDS listener from the descriptors inherited across exec. It consumes the
// hot-upgrade environment so descendants never re-enter this path.
func RestoreHotUpgradeListeners() (data, mmds net.Listener, err error) {
	if !HotUpgradeActive() {
		return nil, nil, fmt.Errorf("proxy: hot upgrade: environment absent")
	}
	dataRaw := os.Getenv(hotUpgradeDataFD)
	dataFD, convErr := strconv.Atoi(dataRaw)
	if convErr != nil || dataFD < 3 {
		return nil, nil, fmt.Errorf("proxy: hot upgrade: invalid data fd %q", dataRaw)
	}
	data, err = listenerFromFD(dataFD, "proxy-data-hot")
	if err != nil {
		return nil, nil, fmt.Errorf("proxy: hot upgrade: restore data listener: %w", err)
	}
	if mmdsRaw := os.Getenv(hotUpgradeMMDSFD); mmdsRaw != "" {
		mmdsFD, convErr := strconv.Atoi(mmdsRaw)
		if convErr != nil || mmdsFD < 3 {
			_ = data.Close()
			return nil, nil, fmt.Errorf("proxy: hot upgrade: invalid mmds fd %q", mmdsRaw)
		}
		mmds, err = listenerFromFD(mmdsFD, "proxy-mmds-hot")
		if err != nil {
			_ = data.Close()
			return nil, nil, fmt.Errorf("proxy: hot upgrade: restore mmds listener: %w", err)
		}
	}
	_ = os.Unsetenv(hotUpgradeEnv)
	_ = os.Unsetenv(hotUpgradeDataFD)
	_ = os.Unsetenv(hotUpgradeMMDSFD)
	return data, mmds, nil
}
