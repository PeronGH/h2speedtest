# h2speedtest

Download speed test over a single HTTP/2 connection with multiple concurrent streams.

## Usage

```
go install github.com/PeronGH/h2speedtest@latest
h2speedtest [-n streams] [-t duration] [--resolve ip] <url>
```

- `-n` concurrent streams (default 100)
- `-t` test duration (default 10s)
- `--resolve` connect to this IP instead of resolving the URL host (SNI and `Host` still use the URL)

Connecting is limited to 5s.

The URL is requested repeatedly on every stream until the duration elapses, with compression disabled and response bodies discarded. The rate counts all bytes on the wire (TLS and HTTP/2 framing included). `https://` URLs require h2 via ALPN; `http://` URLs use HTTP/2 with prior knowledge (h2c).

```
h2speedtest -n 4 -t 5s https://cloudflare.com/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1
```
