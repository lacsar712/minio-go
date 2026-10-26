/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2026 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// connTracker records the peer (TCP) address of every request a test
// server receives, so a test can assert that a set of operations used one
// concrete connection.
type connTracker struct {
	mu       sync.Mutex
	conns    map[string]struct{}
	requests int64
}

func (ct *connTracker) record(remote string) {
	ct.mu.Lock()
	ct.conns[remote] = struct{}{}
	ct.mu.Unlock()
	atomic.AddInt64(&ct.requests, 1)
}

func (ct *connTracker) uniqueConns() int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return len(ct.conns)
}

func newFollowTestClient(t *testing.T, serverURL string, transport http.RoundTripper, secure bool) *Client {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	opts := &Options{
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Secure: secure,
		Region: "us-east-1",
	}
	if transport != nil {
		opts.Transport = transport
	}
	c, err := New(u.Host, opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// scopedGet issues one simple GET through executeMethod, the same path all
// high level APIs use, and fully closes the response.
func scopedGet(t *testing.T, c *Client, ctx context.Context) {
	t.Helper()
	resp, err := c.executeMethod(ctx, http.MethodGet, requestMetadata{})
	if err != nil {
		t.Fatalf("unexpected request error: %v", err)
	}
	closeResponse(resp)
}

func startFollowServer(t *testing.T, h http.HandlerFunc) (*httptest.Server, *connTracker) {
	t.Helper()
	ct := &connTracker{conns: make(map[string]struct{})}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct.record(r.RemoteAddr)
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, ct
}

// TestFollowScopePinsConnection: all requests inside one scope share the
// single connection acquired for the first successful request.
func TestFollowScopePinsConnection(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		scopedGet(t, c, ctx)
	}
	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("expected exactly 1 connection inside scope, saw %d", got)
	}
	if got := atomic.LoadInt64(&ct.requests); got != 6 {
		t.Fatalf("expected 6 requests to reach the server, saw %d", got)
	}
}

// TestFollowScopeRetriesStayOnConnection: every retry hop uses the bound
// connection, including transient 5xx responses.
func TestFollowScopeRetriesStayOnConnection(t *testing.T) {
	var attempts int64
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt64(&attempts, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	scopedGet(t, c, ctx)

	if got := atomic.LoadInt64(&attempts); got != 3 {
		t.Fatalf("expected 3 attempts, saw %d", got)
	}
	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("retry hops must stay on one connection, saw %d", got)
	}
}

// TestFollowScopeBrokenConnectionFails: once bound, a dead connection fails
// the next operation immediately; no new connection is opened and no retry
// happens. Unscoped operations on the same client keep working.
func TestFollowScopeBrokenConnectionFails(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/close" {
			// Ask the server to close after this reply.
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	scope := c.NewFollowScope()
	ctx, err := scope.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	closeReq, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/close", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.do(closeReq)
	if err != nil {
		t.Fatalf("binding request failed: %v", err)
	}
	resp.Body.Close()
	requestsBefore := atomic.LoadInt64(&ct.requests)

	// Give the transport a moment to observe the server-side close.
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, lastErr = c.executeMethod(ctx, http.MethodGet, requestMetadata{})
		if errors.Is(lastErr, ErrFollowConnUnavailable) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(lastErr, ErrFollowConnUnavailable) {
		t.Fatalf("expected ErrFollowConnUnavailable on dead bound connection, got: %v", lastErr)
	}
	if got := atomic.LoadInt64(&ct.requests); got != requestsBefore {
		t.Fatalf("no request may reach the server on a dead bound connection, saw %d new", got-requestsBefore)
	}
	scope.Exit()

	// Ordinary traffic is unaffected and reconnects through the pool.
	scopedGet(t, c, context.Background())
}

// TestFollowScopeBusyWhileStreamOpen: while one scoped request is in
// flight / its body is open, a second scoped request fails fast instead of
// queueing for the connection or opening another one.
func TestFollowScopeBusyWhileStreamOpen(t *testing.T) {
	streamStarted := make(chan struct{})
	releaseStream := make(chan struct{})
	srv, _ := startFollowServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("partial"))
		flusher.Flush()
		close(streamStarted)
		<-releaseStream
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	scope := c.NewFollowScope()
	ctx, err := scope.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	streamReq, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	var (
		streamResp *http.Response
		streamErr  error
		streamDone = make(chan struct{})
	)
	go func() {
		streamResp, streamErr = c.do(streamReq)
		close(streamDone)
		<-releaseStream
	}()
	<-streamStarted
	<-streamDone
	if streamErr != nil {
		t.Fatalf("streaming request failed: %v", streamErr)
	}

	// The bound connection is occupied by the open stream.
	_, err = c.executeMethod(ctx, http.MethodGet, requestMetadata{})
	if !errors.Is(err, ErrFollowConnBusy) {
		t.Fatalf("expected ErrFollowConnBusy while stream is open, got: %v", err)
	}

	close(releaseStream)
	streamResp.Body.Close()
	scope.Exit()
}

// TestFollowScopeExitReleasesAndReenter: exiting drops the binding and the
// scope can be entered again; later traffic works through ordinary rules.
func TestFollowScopeExitReleasesAndReenter(t *testing.T) {
	srv, _ := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	scope := c.NewFollowScope()
	ctx, err := scope.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	scopedGet(t, c, ctx)
	scope.Exit()
	// Exit is idempotent.
	scope.Exit()

	// Stale context after exit behaves like an unscoped one.
	scopedGet(t, c, ctx)

	// Re-arming the scope after exit is allowed.
	ctx2, err := scope.Enter(context.Background())
	if err != nil {
		t.Fatalf("re-enter after exit should succeed: %v", err)
	}
	scopedGet(t, c, ctx2)
	scope.Exit()
}

// TestFollowScopeReentryFails: re-entering an active scope from the same
// task fails, as does a concurrent enter by another task.
func TestFollowScopeReentryFails(t *testing.T) {
	scope := &FollowScope{}
	ctx, err := scope.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scope.Enter(ctx); !errors.Is(err, ErrFollowScopeActive) {
		t.Fatalf("expected ErrFollowScopeActive on re-entry, got: %v", err)
	}

	// Two tasks racing to enter: exactly one wins.
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var winners, losers int
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := scope.Enter(context.Background())
			mu.Lock()
			if errors.Is(err, ErrFollowScopeActive) {
				losers++
			} else if err == nil {
				winners++
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if winners != 0 || losers != 2 {
		t.Fatalf("an active scope must reject both racing enters, got winners=%d losers=%d", winners, losers)
	}
}

// TestFollowScopeParallelScopesIndependent: two sibling scopes each bind
// their own connection and never influence each other.
func TestFollowScopeParallelScopesIndependent(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	scopeA := c.NewFollowScope()
	ctxA, err := scopeA.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	scopeB := c.NewFollowScope()
	ctxB, err := scopeB.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for range 4 {
		scopedGet(t, c, ctxA)
		scopedGet(t, c, ctxB)
	}
	scopeA.Exit()
	scopeB.Exit()

	if got := ct.uniqueConns(); got != 2 {
		t.Fatalf("expected two independent connections for two sibling scopes, saw %d", got)
	}
}

// TestFollowScopeNestedSeesOuterBinding: an inner scope with no successful
// request yet uses the outer bound connection; after the inner exits the
// outer binding is still on the same connection.
func TestFollowScopeNestedSeesOuterBinding(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	outer := c.NewFollowScope()
	ctxOuter, err := outer.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	scopedGet(t, c, ctxOuter) // binds outer

	inner := c.NewFollowScope()
	ctxInner, err := inner.Enter(ctxOuter)
	if err != nil {
		t.Fatal(err)
	}
	scopedGet(t, c, ctxInner) // sees and joins the outer binding
	inner.Exit()

	scopedGet(t, c, ctxOuter) // outer binding survives the inner scope
	outer.Exit()

	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("nested scopes must share the outer connection, saw %d", got)
	}
}

// TestFollowScopeNestedInnerBindsFresh: when no outer binding exists yet,
// the first request inside the inner scope binds the inner scope only; once
// the inner exits, the still-unbound outer scope dials normally.
func TestFollowScopeNestedInnerBindsFresh(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	outer := c.NewFollowScope()
	ctxOuter, err := outer.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	inner := c.NewFollowScope()
	ctxInner, err := inner.Enter(ctxOuter)
	if err != nil {
		t.Fatal(err)
	}
	scopedGet(t, c, ctxInner) // first ever success binds the inner scope
	inner.Exit()

	scopedGet(t, c, ctxOuter) // outer was never bound: normal acquisition
	outer.Exit()

	if got := ct.uniqueConns(); got != 2 {
		t.Fatalf("expected the unbound outer scope to acquire a new connection, saw %d", got)
	}
}

// TestFollowScopePreBindFailureRetries: a failed attempt before the first
// successful send binds nothing and the regular retry path may connect
// again; once a send succeeds the scope binds to that connection.
func TestFollowScopePreBindFailureRetries(t *testing.T) {
	var attempts int64
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt64(&attempts, 1) == 1 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("server does not support hijacking")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	scopedGet(t, c, ctx)

	if got := atomic.LoadInt64(&attempts); got != 2 {
		t.Fatalf("expected a retry after the pre-bind failure, saw %d attempts", got)
	}
	// Bound after the successful retry; the next request stays on it.
	scopedGet(t, c, ctx)
	if got := ct.uniqueConns(); got != 2 {
		t.Fatalf("expected one connection per attempt (failed + bound), saw %d", got)
	}
}

// TestFollowScopeTargetMismatch: a bound scope refuses a hop to another
// host instead of connecting there.
func TestFollowScopeTargetMismatch(t *testing.T) {
	srv1, _ := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv2, _ := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tr, err := DefaultTransport(false)
	if err != nil {
		t.Fatal(err)
	}
	ft := newFollowTransport(tr).(*followTransport)

	scope := &FollowScope{}
	ctx, err := scope.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	req1, err := http.NewRequestWithContext(ctx, http.MethodGet, srv1.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp1, err := ft.RoundTrip(req1)
	if err != nil {
		t.Fatalf("binding request failed: %v", err)
	}
	resp1.Body.Close()

	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, srv2.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ft.RoundTrip(req2); !errors.Is(err, ErrFollowTargetChanged) {
		t.Fatalf("expected ErrFollowTargetChanged when switching hosts, got: %v", err)
	}
	scope.Exit()
}

// TestFollowScopeConcurrentFanOutGuard: operations that fan requests out
// concurrently fail up front while a scope is active.
func TestFollowScopeConcurrentFanOutGuard(t *testing.T) {
	srv, _ := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.putObjectMultipartStreamParallel(ctx, "bucket", "object",
		bytes.NewReader(make([]byte, 1024)), 1024, PutObjectOptions{}); !errors.Is(err, ErrFollowConcurrent) {
		t.Fatalf("parallel multipart stream: expected ErrFollowConcurrent, got %v", err)
	}

	if _, err := c.putObjectMultipartStreamFromReadAt(ctx, "bucket", "object",
		bytes.NewReader(make([]byte, 1024)), 1024, PutObjectOptions{}); !errors.Is(err, ErrFollowConcurrent) {
		t.Fatalf("ReadAt multipart stream: expected ErrFollowConcurrent, got %v", err)
	}

	if _, _, err := c.getObjectParallel(ctx, "bucket", "object", GetObjectOptions{}); !errors.Is(err, ErrFollowConcurrent) {
		t.Fatalf("parallel get: expected ErrFollowConcurrent, got %v", err)
	}
}

// TestFollowScopeCoreSharesRules: a scope created on a Core follows the
// exact same pinning rules as one on a Client.
func TestFollowScopeCoreSharesRules(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := NewCore(u.Host, &Options{
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, err := core.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := core.httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("Core scope must pin to one connection, saw %d", got)
	}
}

// TestFollowScopeTLSPinsConnection: pinning works over HTTPS with a
// caller-provided *http.Transport carrying custom RootCAs (the cloned
// transport must preserve the TLS configuration).
func TestFollowScopeTLSPinsConnection(t *testing.T) {
	ct := &connTracker{conns: make(map[string]struct{})}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct.record(r.RemoteAddr)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}
	c := newFollowTestClient(t, srv.URL, tr, true)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		scopedGet(t, c, ctx)
	}
	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("expected 1 TLS connection inside scope, saw %d", got)
	}
}

// TestFollowScopeOpaqueCustomTransport: a custom RoundTripper that is not an
// *http.Transport keeps serving scoped traffic (host affinity and busy/fatal
// semantics still apply).
func TestFollowScopeOpaqueCustomTransport(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	base, err := DefaultTransport(false)
	if err != nil {
		t.Fatal(err)
	}
	var called int64
	opaque := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt64(&called, 1)
		return base.RoundTrip(req)
	})
	c := newFollowTestClient(t, srv.URL, opaque, false)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		scopedGet(t, c, ctx)
	}
	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("opaque transport scope expected a single observed connection, saw %d", got)
	}
	if got := atomic.LoadInt64(&called); got != 3 {
		t.Fatalf("expected all requests to pass through the custom RoundTripper, saw %d", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// TestNewPinnedTransportDialGuard: the pinned transport dials through the
// original dial function exactly once; any further dial attempt (the bound
// connection is gone) is rejected instead of opening a new connection.
func TestNewPinnedTransportDialGuard(t *testing.T) {
	base, err := DefaultTransport(false)
	if err != nil {
		t.Fatal(err)
	}
	var realDials int64
	base.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		atomic.AddInt64(&realDials, 1)
		c1, c2 := net.Pipe()
		_ = c2
		return c1, nil
	}

	tr, lease := newPinnedTransport(base)

	c1, err := tr.DialContext(context.Background(), "tcp", "127.0.0.1:1")
	if err != nil {
		t.Fatalf("first dial must succeed via the original dialer: %v", err)
	}
	if c1 == nil {
		t.Fatal("first dial returned no connection")
	}
	if got := atomic.LoadInt64(&realDials); got != 1 {
		t.Fatalf("expected one real dial, saw %d", got)
	}

	if _, err := tr.DialContext(context.Background(), "tcp", "127.0.0.1:1"); !errors.Is(err, errFollowNoNewDial) {
		t.Fatalf("second dial must be rejected, got: %v", err)
	}
	if !lease.dead.Load() {
		t.Fatal("rejected dial must mark the lease dead")
	}
	if got := atomic.LoadInt64(&realDials); got != 1 {
		t.Fatalf("a rejected dial must never reach the real dialer, saw %d", got)
	}
	_ = c1.Close()
}

// TestNewPinnedTransportHTTP2Isolation: an explicitly configured h2 upgrade
// map is not shared with the base transport; the clone gets its own isolated
// h2 stack, while an explicit empty map (h2 disabled) is preserved.
func TestNewPinnedTransportHTTP2Isolation(t *testing.T) {
	base, err := DefaultTransport(true)
	if err != nil {
		t.Fatal(err)
	}
	base.TLSNextProto = map[string]func(authority string, c *tls.Conn) http.RoundTripper{
		"h2": func(string, *tls.Conn) http.RoundTripper { return nil },
	}

	tr, _ := newPinnedTransport(base)
	if tr.TLSNextProto != nil {
		t.Fatalf("cloned transport must not share the base h2 upgrade funcs, got %v", tr.TLSNextProto)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Fatal("cloned transport must re-enable h2 on its own isolated stack")
	}

	base.TLSNextProto = map[string]func(authority string, c *tls.Conn) http.RoundTripper{}
	tr2, _ := newPinnedTransport(base)
	if tr2.TLSNextProto == nil {
		t.Fatal("an explicit empty TLSNextProto map (h2 disabled) must be preserved")
	}
}

// TestFollowScopePresignUnaffected: presigning never sends a request and
// behaves identically inside a scope.
func TestFollowScopePresignUnaffected(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	u, err := c.PresignedGetObject(ctx, "bucket", "object", time.Hour, nil)
	if err != nil {
		t.Fatalf("presign inside scope failed: %v", err)
	}
	if u == nil || u.String() == "" {
		t.Fatal("presign returned an empty URL")
	}
	if got := atomic.LoadInt64(&ct.requests); got != 0 {
		t.Fatalf("presign must not send any request, saw %d", got)
	}
}

// TestFollowScopeEndToEndPutGet: the public PutObject/GetObject API chain
// (cold bucket location lookup, signed PUT, signed GET with open stream)
// stays on the single bound connection end to end.
func TestFollowScopeEndToEndPutGet(t *testing.T) {
	objectData := []byte("follow-scope-payload")
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Query().Has("location"):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			w.Header().Set("ETag", `"deadbeef"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(objectData)))
			w.Header().Set("ETag", `"deadbeef"`)
			w.WriteHeader(http.StatusOK)
		default:
			w.Header().Set("ETag", `"deadbeef"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(objectData)
		}
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	ctx, err := c.NewFollowScope().Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.PutObject(ctx, "bucket", "object",
		bytes.NewReader(objectData), int64(len(objectData)), PutObjectOptions{}); err != nil {
		t.Fatalf("PutObject inside scope failed: %v", err)
	}

	obj, err := c.GetObject(ctx, "bucket", "object", GetObjectOptions{})
	if err != nil {
		t.Fatalf("GetObject inside scope failed: %v", err)
	}
	got, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("reading object failed: %v", err)
	}
	obj.Close()
	if !bytes.Equal(got, objectData) {
		t.Fatalf("object payload mismatch: got %q want %q", got, objectData)
	}

	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("PutObject+GetObject chain must use one connection, saw %d", got)
	}
}

// TestFollowScopeUnscopedPathUnchanged: without a scope the wrapper is a
// pass-through and requests pool normally.
func TestFollowScopeUnscopedPathUnchanged(t *testing.T) {
	srv, ct := startFollowServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	c := newFollowTestClient(t, srv.URL, nil, false)

	for range 3 {
		scopedGet(t, c, context.Background())
	}
	if got := ct.uniqueConns(); got != 1 {
		t.Fatalf("ordinary keep-alive traffic should still pool, saw %d conns", got)
	}

	// The client transport is the installed wrapper, not a swap of pools.
	if _, ok := c.httpClient.Transport.(*followTransport); !ok {
		t.Fatalf("expected followTransport wrapper, got %T", c.httpClient.Transport)
	}
}
