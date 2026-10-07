# RemoteOps - IE3090 Network Programming (Registration No. IT24103381)

A remote system monitoring tool in C over TCP/IP (control channel) and UDP
(periodic monitoring): an **Agent** (server) and a **Controller** (client).

## Personalised values (calculated from IT24103381)

| Item | Calculation | Value |
|---|---|---|
| Agent TCP port | 7000 + first four digits of 24103381 (2410) | **9410** |
| Source files | last three digits 381 | `agent_381.c`, `controller_381.c`, `Makefile_381` |
| SID tag | last four digits 3381, reversed | **SID:1833** |
| Auth token | "OPS-" + 3381 | **OPS-3381** |
| Log file | `remoteops_<regno>.log` | `remoteops_IT24103381.log` |
| Storage path | `./agentfiles/<regno>/<filename>` | `./agentfiles/IT24103381/<filename>` |
| Archive | `IE3090_<regno>.zip` | `IE3090_IT24103381.zip` |

## Files

| File | Purpose |
|---|---|
| `agent_381.c` | Agent (server): TCP control channel, UDP monitor, logging |
| `controller_381.c` | Controller (client): interactive commands, PUT/GET, UDP receiver |
| `Makefile_381` | Builds both programs |
| `test_381.sh` | Automated checks |
| `remoteops_IT24103381.log` | Sample Agent log from my own run |

## Build

Needs Linux with gcc and make (tested on CentOS Stream 10, gcc 14.4.1).

```
make -f Makefile_381
```

## Run

Terminal 1 (Agent):
```
./agent_381
```
Terminal 2 (Controller):
```
./controller_381 [host] [port]      # defaults 127.0.0.1 9410
remoteops> AUTH OPS-3381
remoteops> SYSINFO
remoteops> LISTPROC
remoteops> EXEC HOSTNAME
remoteops> PUT report.txt           # uploads, prints throughput
remoteops> GET report.txt           # saves as dl_report.txt
remoteops> MONITOR START 5000       # UDP stats arrive every 2 s
remoteops> MONITOR STOP
remoteops> QUIT
```

## Design summary

- **Concurrency:** one detached pthread per Controller connection.
- **Framing:** per-session receive buffer; handles partial lines, several
  lines per `recv()`, and exact `<filesize>` byte counting for PUT/GET.
- **Security:** AUTH required first; EXEC uses a fixed table (client text is
  never passed to a shell); filenames restricted to `[A-Za-z0-9._-]`, no
  leading dot (no path traversal); uploads capped at 10 MB.
- **UDP monitor:** a per-session thread sends `SYSINFO <cpu> <mem_mb> <uptime> SID:1833`
  every 2 seconds to the Controller's IP on the requested port.
- **Logging:** every connection, command, transfer and disconnect is logged
  with a timestamp to `remoteops_IT24103381.log` (token is never logged).
- **Optional extension implemented:** throughput (bytes/second) reported by
  the Controller for PUT and GET.

