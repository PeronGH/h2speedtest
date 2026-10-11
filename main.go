package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func (c countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.n.Add(int64(n))
	return n, err
}

type console struct {
	mu     sync.Mutex
	status bool
}

func (c *console) setStatus(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprint(os.Stderr, "\r\x1b[K"+line)
}

func (c *console) clearStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status {
		fmt.Fprint(os.Stderr, "\r\x1b[K")
	}
}

func (c *console) warn(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status {
		fmt.Fprint(os.Stderr, "\r\x1b[K")
	}
	fmt.Fprintf(os.Stderr, "warning: "+format+"\n", args...)
}

func main() {
	streams := flag.Int("n", 100, "number of concurrent streams")
	timeout := flag.Duration("t", 10*time.Second, "test duration")
	resolve := flag.String("resolve", "", "connect to this `ip` instead of resolving the URL host")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [flags] <url>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 || *streams < 1 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(flag.Arg(0), *streams, *timeout, *resolve); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(rawURL string, streams int, timeout time.Duration, resolve string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	port := u.Port()
	switch u.Scheme {
	case "https":
		if port == "" {
			port = "443"
		}
	case "http":
		if port == "" {
			port = "80"
		}
	default:
		return fmt.Errorf("unsupported scheme %q", u.Scheme)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var wire atomic.Int64
	dialer := &net.Dialer{}
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	tr := &http.Transport{
		Protocols:          protocols,
		DisableCompression: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if resolve != "" {
				addr = net.JoinHostPort(resolve, port)
			}
			c, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return countingConn{Conn: c, n: &wire}, nil
		},
	}

	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	cc, err := tr.NewClientConn(dialCtx, u.Scheme, net.JoinHostPort(u.Hostname(), port))
	cancelDial()
	if err != nil {
		return err
	}
	defer cc.Close()

	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	con := &console{}
	if fi, err := os.Stderr.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		con.status = true
	}

	tmpl, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	tmpl.Header.Set("Accept-Encoding", "identity")

	// The hook fires as soon as a stream ends on the wire, which is before
	// its buffered body has been drained, so refilling from it keeps exactly
	// N streams open without waiting on body readers.
	wake := make(chan struct{}, 1)
	cc.SetStateHook(func(*http.ClientConn) {
		select {
		case wake <- struct{}{}:
		default:
		}
	})

	var warnEncoding sync.Once
	var streamErrors atomic.Int64
	var wg sync.WaitGroup
	fill := func() {
		for ctx.Err() == nil && cc.InFlight() < streams && cc.Reserve() == nil {
			wg.Go(func() {
				err := fetch(cc, tmpl.Clone(ctx), con, &warnEncoding)
				switch {
				case err == nil || ctx.Err() != nil:
				case cc.Err() != nil || errors.As(err, new(statusError)):
					cancel(err)
				default:
					streamErrors.Add(1)
				}
			})
		}
	}

	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	lastTime, lastBytes := start, wire.Load()
	fill()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-wake:
			fill()
		case now := <-ticker.C:
			if !con.status {
				continue
			}
			b := wire.Load()
			cur := rate(b-lastBytes, now.Sub(lastTime))
			lastTime, lastBytes = now, b
			con.setStatus(fmt.Sprintf("%5.1fs  %s  %s  avg %s  streams %d/%d  errors %d",
				now.Sub(start).Seconds(), formatBytes(b), cur, rate(b, now.Sub(start)), cc.InFlight(), streams, streamErrors.Load()))
		}
	}
	elapsed := time.Since(start)
	b := wire.Load()
	wg.Wait()
	con.clearStatus()

	if err := context.Cause(ctx); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	fmt.Printf("%s  %s in %.1fs  avg %s  %d streams  %d errors\n", rawURL, formatBytes(b), elapsed.Seconds(), rate(b, elapsed), streams, streamErrors.Load())
	return nil
}

type statusError struct{ status string }

func (e statusError) Error() string { return "unexpected status " + e.status }

func fetch(cc *http.ClientConn, req *http.Request, con *console, warnEncoding *sync.Once) error {
	resp, err := cc.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return statusError{resp.Status}
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		warnEncoding.Do(func() { con.warn("server sent Content-Encoding %q despite identity request", enc) })
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

func rate(bytes int64, d time.Duration) string {
	if d <= 0 {
		return "0.00 Mbit/s"
	}
	bps := float64(bytes) * 8 / d.Seconds()
	switch {
	case bps >= 1e9:
		return fmt.Sprintf("%.2f Gbit/s", bps/1e9)
	default:
		return fmt.Sprintf("%.2f Mbit/s", bps/1e6)
	}
}

func formatBytes(b int64) string {
	f := float64(b)
	switch {
	case f >= 1e9:
		return fmt.Sprintf("%.2f GB", f/1e9)
	case f >= 1e6:
		return fmt.Sprintf("%.2f MB", f/1e6)
	case f >= 1e3:
		return fmt.Sprintf("%.2f kB", f/1e3)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
