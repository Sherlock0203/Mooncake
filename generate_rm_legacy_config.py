#!/usr/bin/env python3
"""Generate transfer-engine-rm_legacy.json from /sys/class/ubcore/.

Scans UB devices, reads each device's EID from sysfs, and fills the RM
Legacy performance-test configuration template with the discovered
identities. Run it on the Linux machine under test:

    python3 generate_rm_legacy_config.py                       # ./transfer-engine-rm_legacy.json
    python3 generate_rm_legacy_config.py --output /tmp/rm.json

Exit codes: 0 on success; 1 when the UB-core sysfs tree or a device EID
cannot be read (fix the environment or fill the template manually).
"""

import argparse
import json
import os
import sys

UBCORE_PATH = "/sys/class/ubcore"

# Driver-specific sysfs exposure is not stable across UMDK versions, so the
# EID and transport-mode readers probe a list of candidate file names.
EID_CANDIDATES = ("eid", "eid_address", "eid_addr")
MODES_CANDIDATES = ("transports", "transport_modes", "mode")

# RC=1, RM=2; 3 == RC|RM. The RM capability gate only requires the RM bit,
# so the default is safe for both mixed and RM-only providers.
DEFAULT_TRANSPORT_MODES = 3


def read_first_line(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read().strip()
    except OSError:
        return ""


def list_ub_devices():
    if not os.path.isdir(UBCORE_PATH):
        sys.exit(
            f"ERROR: {UBCORE_PATH} not found. Is the uburma/UB-core kernel "
            "module loaded and UMDK installed?"
        )
    devices = sorted(
        entry
        for entry in os.listdir(UBCORE_PATH)
        if os.path.isdir(os.path.join(UBCORE_PATH, entry))
        or os.path.islink(os.path.join(UBCORE_PATH, entry))
    )
    if not devices:
        sys.exit(f"ERROR: no UB devices found under {UBCORE_PATH}")
    return devices


def read_eid(device):
    for name in EID_CANDIDATES:
        value = read_first_line(os.path.join(UBCORE_PATH, device, name))
        if value:
            return value, name
    return "", ""


def read_transport_modes(device):
    for name in MODES_CANDIDATES:
        raw = read_first_line(os.path.join(UBCORE_PATH, device, name))
        if not raw:
            continue
        try:
            return int(raw, 0), name
        except ValueError:
            continue
    return DEFAULT_TRANSPORT_MODES, "DEFAULT(3=RC|RM)"


def build_template():
    """RM Legacy performance config; device lists are filled by main()."""
    return {
        "local_segment_name": "",
        "metadata_type": "p2p",
        "metadata_servers": "127.0.0.1:2379",
        "rpc_server_hostname": "127.0.0.1",
        "rpc_server_port": 0,
        "topology": {"rdma_whitelist": [], "rdma_blacklist": []},
        "log_level": "info",
        "max_failover_attempts": 3,
        "enable_auto_failover_on_poll": True,
        "enable_progress_worker": False,
        "metrics": {
            "enabled": True,
            "http_port": 9100,
            "http_host": "0.0.0.0",
            "http_server_threads": 2,
            "report_interval_seconds": 30,
        },
        "transports": {
            "rdma": {"enable": False},
            "ub": {
                "enable": True,
                "device_filter": [],
                "worker_count": 4,
                "poller_count": 1,
                "jfc_per_context": 1,
                "jetty_per_endpoint": 4,
                "max_endpoints": 4096,
                "slice_size": 65536,
                "max_retries": 3,
                "slice_timeout_ms": 10000,
                "endpoint_cooldown_ms": 30000,
                "enable_bandwidth_estimation": True,
                "enable_notifications": False,
                "runtime_profile": "rm_legacy",
                "trans_mode": "rm",
                "physical_only": True,
                "device_identities": [],
                "jetty_pool": {"enable": False, "force_disable": True},
                "probe": {
                    "enabled": False,
                    "interval": 1000,
                    "timeout": 500,
                    "backoff_max": 30000,
                    "peer_budget": 1,
                    "recover_success_threshold": 1,
                    "failure_threshold": 3,
                    "stop_on_recovery": True,
                },
            },
            "tcp": {
                "enable": True,
                "max_retry_count": 3,
                "retry_base_delay_ms": 100,
                "retry_max_delay_ms": 2000,
                "max_concurrent_tasks": 16,
            },
            "hp_tcp": {"enable": False},
            "gds": {"enable": False},
            "shm": {"enable": True, "cxl_mount_path": ""},
            "mnnvl": {"enable": False},
            "ascend_direct": {"enable": False},
            "hylink": {"enable": False},
            "memfabric": {"enable": False},
        },
        "policy": [
            {
                "name": "default_memory",
                "segment_type": "memory",
                "devices": [],
                "transports": ["ub", "shm", "tcp"],
            },
            {
                "name": "file_storage",
                "segment_type": "file",
                "transports": ["io_uring", "tcp"],
            },
        ],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate the RM Legacy transfer-engine.json from sysfs."
    )
    parser.add_argument(
        "--output",
        default="transfer-engine-rm_legacy.json",
        help="output file path (default: ./transfer-engine-rm_legacy.json)",
    )
    args = parser.parse_args()

    devices = list_ub_devices()

    identities = []
    for device in devices:
        eid, eid_source = read_eid(device)
        if not eid:
            sys.exit(
                f"ERROR: cannot read EID for device '{device}' "
                f"(tried: {', '.join(EID_CANDIDATES)}). Fill it manually in "
                "the template or check the UMDK version."
            )
        modes, modes_source = read_transport_modes(device)
        print(
            f"[discover] {device}: eid={eid} (sysfs:{eid_source or 'n/a'}), "
            f"transport_modes={modes} ({modes_source})"
        )
        identities.append(
            {
                "native_device_name": device,
                "native_device_path": f"{UBCORE_PATH}/{device}",
                "eid": eid,
                "eid_index": 0,
                "transport_modes": modes,
                "kind": "physical",
            }
        )

    config = build_template()
    config["transports"]["ub"]["device_filter"] = devices
    config["transports"]["ub"]["device_identities"] = identities
    config["policy"][0]["devices"] = devices

    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=4)
        handle.write("\n")

    print(
        f"[write] {args.output}: {len(devices)} UB device(s) "
        f"({', '.join(devices)}), runtime_profile=rm_legacy"
    )
    print(
        "[note] eid_index defaults to 0; adjust it if the device exposes "
        "multiple EID groups. Regenerate on EVERY node: each endpoint fills "
        "in its own device identities."
    )


if __name__ == "__main__":
    main()
