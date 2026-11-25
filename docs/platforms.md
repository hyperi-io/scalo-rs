# Platforms

Linux is scalo's core platform: the DFE services run there, and every feature does. macOS and Windows run the features a host agent uses: config, logging, metrics, memory, shutdown, the transports, the spool and the tiered sink. Where an OS lacks what Linux has, scalo takes the smallest fallback, and this table says which.

| Area | Linux | macOS | Windows |
| --- | --- | --- | --- |
| `MemoryGuard` usage | cgroup v2, then v1, then `/proc/self/status` `VmRSS` | the process's resident size from `proc_pidinfo` | the process's working set from `GetProcessMemoryInfo` |
| Shutdown signals | SIGTERM and SIGINT | SIGTERM and SIGINT | Ctrl+C, a console close and a system shutdown. A Windows service calls `shutdown::trigger()` on its own stop control |
| Config reload | file change, SIGHUP and the periodic timer | the same | file change and the periodic timer, with no SIGHUP |
| Spool directory fsync | flushed after a segment is created or renamed | the same, skipping the sealed system volume's refusal | none exists, so the segment's own flush is the last step |
| Spool lock | an OS lock on the lock file, dropped with a dead process | the same | the same |
| Spool directory mode | `0700` | `0700` | the folder's ACL |
| `disk_aware` free space | `statvfs` | `statvfs` | `GetDiskFreeSpaceExW` |
| Process metrics | CPU, memory, start time, `process_open_fds` | CPU, memory, start time | CPU, memory, start time, `process_open_handles` |
| Container metrics | cgroup v1 and v2 | none | none |
| `version_check` instance id | the Kubernetes CA, else `/etc/machine-id` outside a container, else a stored id | a stored id | a stored id |
| `RuntimePaths::run_dir` | `XDG_RUNTIME_DIR`, else the temp dir | the temp dir | the temp dir |
| `proxy: system` | the proxy environment variables | those, then the System Configuration proxies | those, then the WinHTTP machine proxy |
| Managed config layer | none | MDM managed preferences | Group Policy in the registry |

On Windows `process_virtual_memory_bytes` is the private commit (sysinfo's `PrivateUsage`), not the address space Linux and macOS report, so don't chart it against resident memory across OSes.

Two defaults are Linux paths, so set them on macOS and Windows: `DEFAULT_SPOOL_ROOT` (`/var/spool/scalo`), which the DLQ file and output paths start from, and `geoip_download.data_dir` (`/var/lib/geoip`).

Container metrics and the Kubernetes and container detection in `env::Environment` read Linux paths, so off Linux they find nothing and bare metal. `deployment` writes Linux container artefacts and builds on any OS. macOS builds librdkafka from source for `transport-kafka` ([deployment/native-deps.md](deployment/native-deps.md)).
