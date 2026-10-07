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
| `remoteops_IT24103381.log` | Sample Agent log from my own run |

## Build

Needs Linux with gcc and make (tested on CentOS Stream 10, gcc 14.4.1).

    make -f Makefile_381

## Run

Terminal 1 (Agent):

    ./agent_381

Terminal 2 (Controller):

    ./controller_381 127.0.0.1
    remoteops> AUTH OPS-3381
    remoteops> SYSINFO
    remoteops> LISTPROC
    remoteops> EXEC HOSTNAME
    remoteops> PUT report.txt
    remoteops> GET report.txt
    remoteops> MONITOR START 5000
    remoteops> MONITOR STOP
    remoteops> QUIT

## Design summary

- **Concurrency:** one detached pthread per Controller connection.
- **Framing:** per-session receive buffer; handles partial lines,
  several lines per recv(), and exact `<filesize>` byte counting for PUT/GET.
- **Security:** AUTH required first; EXEC uses a fixed table; filenames
  restricted to `[A-Za-z0-9._-]`, no leading dot; uploads capped at 10 MB.
- **UDP monitor:** per-session thread sends SYSINFO every 2 seconds.
- **Logging:** every connection, command, transfer and disconnect logged.
- **Optional extension:** throughput (bytes/second) reported for PUT and GET.

## Author

[Jayarathne A W M R] (IT24103381)
