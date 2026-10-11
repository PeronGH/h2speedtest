# h2speedtest

Download speed test over a single HTTP/2 connection with multiple concurrent streams.

## Usage

```
go install github.com/PeronGH/h2speedtest@latest
h2speedtest [-n streams] [-t duration] <url>
```

- `-n` concurrent streams (default 8)
- `-t` test duration (default 10s)

The URL is requested repeatedly on every stream until the duration elapses, with compression disabled and response bodies discarded. The rate counts all bytes on the wire (TLS and HTTP/2 framing included). `https://` URLs require h2 via ALPN; `http://` URLs use HTTP/2 with prior knowledge (h2c).

```
$ h2speedtest -n 4 -t 5s 'https://speed.cloudflare.com/__down?bytes=100000000'
https://speed.cloudflare.com/__down?bytes=100000000  612.40 MB in 5.0s  avg 979.84 Mbit/s  4 streams
```
