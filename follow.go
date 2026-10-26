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
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
)

// Follow scope fatal errors. They are returned unwrapped through
// errors.Is/errors.As even though the net/http client wraps transport
// errors in *url.Error. The error text is kept free of network phrases so
// that IsNetworkOrHostDown never classifies a pinned-connection failure as
// a client-wide outage.
var (
	// ErrFollowScopeActive is returned when a goroutine tries to enter a
	// FollowScope that has not been exited yet, whether by the same task
	// re-entering it or by a second task entering it concurrently.
	ErrFollowScopeActive = errors.New("minio: follow scope is already active")

	// ErrFollowConnBusy is returned when the connection bound to a follow
	// scope is still occupied by an unfinished request or by an open
	// streaming PutObject/GetObject and another scoped request would have
	// to wait for it or open a second connection.
	ErrFollowConnBusy = errors.New("minio: follow scope connection is occupied by an unfinished request")

	// ErrFollowConnUnavailable is returned when the connection bound to a
	// follow scope broke, hit a protocol error, was evicted or otherwise
	// cannot carry another request. The operation fails immediately
	// instead of silently switching to another connection.
	ErrFollowConnUnavailable = errors.New("minio: follow scope connection is no longer usable")

	// ErrFollowTargetChanged is returned when the current retry or redirect
	// hop would target a different endpoint host, scheme/transport or TLS
	// verification than the connection bound to the follow scope.
	ErrFollowTargetChanged = errors.New("minio: follow scope request would switch endpoint or transport")

	// ErrFollowConcurrent is returned when an operation that fans out
	// concurrent HTTP requests (parallel part uploads or parallel ranged
	// downloads) is started while a follow scope is active. Such an
	// operation cannot stay on the single bound connection.
	ErrFollowConcurrent = errors.New("minio: concurrent fan-out transfers are not supported inside a follow scope")
)

// followCtxKey carries the innermost follow frame in a request context.
type followCtxKey struct{}

// followFrame is one Enter() on the context's stack of nested scopes.
type followFrame struct {
	scope *FollowScope
	prev  *followFrame
}

func followStack(ctx context.Context) *followFrame {
	f, _ := ctx.Value(followCtxKey{}).(*followFrame)
	return f
}

// FollowScope is an explicit, caller-entered range in which every request
// issued through the derived context stays on the same underlying HTTP
// connection.
//
// The first request that is successfully sent binds the connection it used
// to the scope. Nothing is stored on the Client or Core and the regular
// connection pool is left untouched: outside any scope all dialing, pooling,
// retry and redirect behavior is unchanged. Requests issued while the scope
// is unbound still dial or take a connection under the normal rules; a
// failure before the first successful send binds nothing and retries behave
// normally.
//
// Once bound, every later request (including every retry and redirect hop)
// is sent over that one connection. If that connection breaks or the next
// hop would reach a different endpoint host (with a different transport or
// TLS verification), the operation fails with ErrFollowConnUnavailable or
// ErrFollowTargetChanged instead of opening another connection.
//
// Scopes may be nested. An inner scope that has not sent a request yet sees
// (and shares) the connection bound to an enclosing scope; after its own
// first successful send the inner scope keeps using its own binding. An
// exited scope drops its binding; the connection is not closed or pinned by
// the scope and follows ordinary idle, expiry and close rules.
//
// A FollowScope must not be entered by two tasks at once, and a task must
// Exit() before Enter()-ing the same scope again. Parallel fan-out
// transfers (concurrent multipart part uploads and parallel ranged GETs)
// cannot run inside a scope and fail up front.
type FollowScope struct {
	mu sync.Mutex

	// active is set by Enter and cleared by Exit.
	active bool

	// lease is the connection binding, assigned after the first
	// successful request and released by Exit.
	lease *followLease

	// inFlight reserves the scope for the duration of one request,
	// including the lifetime of an open streaming response body. It makes
	// overlapping scoped requests fail fast instead of queueing.
	inFlight atomic.Bool
}

// NewFollowScope returns an idle follow scope for this client. The scope
// carries no client-wide state; Core inherits this method through the
// embedded *Client and scopes created on a Core obey exactly the same rules
// as scopes created on a Client.
func (c *Client) NewFollowScope() *FollowScope {
	return &FollowScope{}
}

// Enter activates the scope and returns a derived context that pins all
// requests issued through it to one connection. It returns
// ErrFollowScopeActive when the scope is already active, whether from a
// concurrent task or a re-entry before Exit.
func (s *FollowScope) Enter(ctx context.Context) (context.Context, error) {
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return ctx, ErrFollowScopeActive
	}
	s.active = true
	s.lease = nil
	s.mu.Unlock()
	s.inFlight.Store(false)

	return context.WithValue(ctx, followCtxKey{}, &followFrame{
		scope: s,
		prev:  followStack(ctx),
	}), nil
}

// Exit leaves the scope and releases its binding. It is safe to defer and
// is a no-op when the scope was not entered. The previously bound
// connection is neither closed nor retained: it returns to ordinary pool
// lifecycle rules.
func (s *FollowScope) Exit() {
	s.mu.Lock()
	lease := s.lease
	s.lease = nil
	s.active = false
	s.mu.Unlock()
	if lease != nil {
		lease.release()
	}
}

func (s *FollowScope) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *FollowScope) currentLease() *followLease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lease
}

// bind attaches the scope to a lease created by (or shared with) an
// enclosing scope. Each binding takes one reference; Exit releases it. It
// is a no-op when the scope was exited while the request was in flight.
func (s *FollowScope) bind(lease *followLease) {
	s.mu.Lock()
	if s.active && s.lease == nil {
		s.lease = lease
		lease.refs.Add(1)
	}
	s.mu.Unlock()
}

// resolveFollow walks the frame stack and returns the innermost active
// scope together with the innermost lease bound anywhere on the active
// stack. An innermost unbound scope therefore observes an outer binding.
func resolveFollow(frame *followFrame) (inner *FollowScope, lease *followLease) {
	for f := frame; f != nil; f = f.prev {
		s := f.scope
		if !s.isActive() {
			// The scope that pushed this frame was exited; contexts
			// derived before Exit must not keep pinning anything.
			continue
		}
		if inner == nil {
			inner = s
		}
		if lease == nil {
			lease = s.currentLease()
		}
	}
	return inner, lease
}

// followActive reports whether any active scope is present on the context.
// It guards operation entry points that fan out concurrently.
func followActive(ctx context.Context) bool {
	for f := followStack(ctx); f != nil; f = f.prev {
		if f.scope.isActive() {
			return true
		}
	}
	return false
}

// followLease is one bound connection shared by the scope(s) bound to it.
type followLease struct {
	scheme string
	host   string

	// tr serves strict pinning when the client transport is a standard
	// *http.Transport; its pool ever contains the one bound connection.
	tr *http.Transport

	// opaque serves clients configured with a custom non-*http.Transport
	// RoundTripper, where TCP connection ownership cannot be controlled.
	// Host affinity, the busy guard and fail-fast semantics still apply;
	// connection identity is additionally observed through httptrace.
	opaque http.RoundTripper

	// remote is the observed peer address of the bound connection on the
	// opaque path, used as a best-effort identity check.
	remote string

	// inFlight is set from request dispatch until the response body is
	// closed, so overlapping scoped requests (or retries while a stream is
	// open) fail immediately instead of queueing for or replacing the
	// connection.
	inFlight atomic.Bool

	// dead is set by the bound connection closing/erroring or by any
	// transport-level failure after binding.
	dead atomic.Bool

	// refs counts scope bindings; the lease never closes the connection on
	// its own - when the last binding goes away references are simply
	// dropped and ordinary idle/expiry rules take over.
	refs atomic.Int32
}

func (l *followLease) release() { l.refs.Add(-1) }

func (l *followLease) markDead() { l.dead.Store(true) }

// followTransport is installed once, at client construction, in front of
// the configured (or default) transport. Requests without an active follow
// scope on their context are delegated verbatim, so the original pool and
// its behavior stay exactly as configured - no transport is swapped when a
// scope is entered.
type followTransport struct {
	base http.RoundTripper

	// baseT is non-nil when base is a standard *http.Transport, enabling
	// strict single-connection pinning.
	baseT *http.Transport
}

func newFollowTransport(base http.RoundTripper) http.RoundTripper {
	ft := &followTransport{base: base}
	if t, ok := base.(*http.Transport); ok {
		ft.baseT = t
	}
	return ft
}

func (t *followTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	frame := followStack(req.Context())
	if frame == nil {
		// Unchanged path for everything outside a follow scope.
		return t.base.RoundTrip(req)
	}
	return t.roundTripFollow(req, frame)
}

func followRequestTarget(req *http.Request) (scheme, host string) {
	host = req.Host
	if host == "" {
		host = req.URL.Host
	}
	return req.URL.Scheme, host
}

// followFatalError reports a terminal follow-scope failure. Its Error text
// is only the stable sentinel text (no embedded network error strings) while
// cause stays inspectable, so IsNetworkOrHostDown cannot mistake it for a
// client-wide outage and errors.Is still matches the sentinel.
type followFatalError struct {
	sentinel error
	cause    error
}

func (e *followFatalError) Error() string { return e.sentinel.Error() }
func (e *followFatalError) Unwrap() error { return e.sentinel }

func followFatal(sentinel, cause error) error {
	return &followFatalError{sentinel: sentinel, cause: cause}
}

// isFollowFatal reports whether the current retry loop must stop.
func isFollowFatal(err error) bool {
	return errors.Is(err, ErrFollowConnBusy) ||
		errors.Is(err, ErrFollowConnUnavailable) ||
		errors.Is(err, ErrFollowTargetChanged) ||
		errors.Is(err, ErrFollowConcurrent)
}

func (t *followTransport) roundTripFollow(req *http.Request, frame *followFrame) (*http.Response, error) {
	inner, lease := resolveFollow(frame)
	if inner == nil {
		// Every frame belongs to an already exited scope; behave as if no
		// scope was present.
		return t.base.RoundTrip(req)
	}

	// Reserve the innermost scope for this round trip until the response
	// body is closed. Overlapping requests fail instead of queueing.
	if !inner.inFlight.CompareAndSwap(false, true) {
		return nil, ErrFollowConnBusy
	}
	clearInner := true
	defer func() {
		if clearInner {
			inner.inFlight.Store(false)
		}
	}()

	scheme, host := followRequestTarget(req)

	if lease != nil {
		// Bound path: every hop must keep the same endpoint, transport
		// configuration (clone) and TLS verification, and use exactly the
		// one leased connection.
		if scheme != lease.scheme || host != lease.host {
			return nil, followFatal(ErrFollowTargetChanged, nil)
		}
		return t.roundTripBound(req, inner, lease, &clearInner)
	}

	// Unbound path: dial or take a connection under the current rules.
	// Nothing binds on failure, so retries remain free to connect anew.
	if t.baseT != nil {
		return t.roundTripFreshTransport(req, inner, scheme, host, &clearInner)
	}
	return t.roundTripFreshOpaque(req, inner, scheme, host, &clearInner)
}

// finishResponse marks a successful dispatch: the innermost unbound scope
// joins (binds) the lease that served it, and the response body releases the
// in-flight reservation only once it is closed - streaming PUT/GET keep the
// connection reserved while they are in progress.
func finishResponse(resp *http.Response, inner *FollowScope, lease *followLease, clearInner *bool) *http.Response {
	inner.bind(lease)
	origBody := resp.Body
	resp.Body = &followBody{
		ReadCloser: origBody,
		onClose: func() {
			lease.inFlight.Store(false)
			inner.inFlight.Store(false)
		},
	}
	*clearInner = false
	return resp
}

func (t *followTransport) roundTripBound(req *http.Request, inner *FollowScope, lease *followLease, clearInner *bool) (*http.Response, error) {
	if lease.dead.Load() {
		return nil, followFatal(ErrFollowConnUnavailable, nil)
	}
	if !lease.inFlight.CompareAndSwap(false, true) {
		return nil, ErrFollowConnBusy
	}

	if lease.opaque != nil {
		return t.opaqueRoundTrip(req, inner, lease, true, clearInner)
	}

	resp, err := lease.tr.RoundTrip(req)
	if err != nil {
		lease.inFlight.Store(false)
		lease.markDead()
		return nil, followFatal(ErrFollowConnUnavailable, err)
	}
	return finishResponse(resp, inner, lease, clearInner), nil
}

// roundTripFreshTransport performs one unbound attempt through a
// single-connection transport cloned from the configured *http.Transport,
// so dialing, proxy and TLS rules are identical. The cloned pool starts
// empty and dials through the same dial functions; only after the response
// arrives is it promoted to the scope lease. A pre-bind failure returns the
// raw error and drops the attempt, leaving current retry behavior intact.
func (t *followTransport) roundTripFreshTransport(req *http.Request, inner *FollowScope, scheme, host string, clearInner *bool) (*http.Response, error) {
	tr, lease := newPinnedTransport(t.baseT)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		// Drop the attempt; any connection it opened is closed by the
		// failed round trip or its own idle handling.
		lease.markDead()
		return nil, err
	}
	lease.scheme = scheme
	lease.host = host
	return finishResponse(resp, inner, lease, clearInner), nil
}

// roundTripFreshOpaque performs one unbound attempt through the custom
// RoundTripper unchanged; binding records only endpoint and connection
// identity, because the raw connection cannot be owned externally.
func (t *followTransport) roundTripFreshOpaque(req *http.Request, inner *FollowScope, scheme, host string, clearInner *bool) (*http.Response, error) {
	lease := &followLease{
		scheme: scheme,
		host:   host,
		opaque: t.base,
	}
	resp, err := t.opaqueRoundTrip(req, inner, lease, false, clearInner)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// opaqueRoundTrip dispatches through a custom RoundTripper, observing the
// served connection via httptrace. bound=false is the unbound attempt (raw
// errors, nothing recorded on failure); bound=true enforces the observed
// connection identity and fails fatally.
func (t *followTransport) opaqueRoundTrip(req *http.Request, inner *FollowScope, lease *followLease, bound bool, clearInner *bool) (*http.Response, error) {
	var mismatch atomic.Bool
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			addr := ""
			if info.Conn != nil {
				addr = info.Conn.RemoteAddr().String()
			}
			if bound {
				if addr != lease.remote {
					mismatch.Store(true)
				}
				return
			}
			lease.remote = addr
		},
	}
	// Compose with any trace already attached (e.g. Options.Trace).
	if prev := httptrace.ContextClientTrace(req.Context()); prev != nil {
		trace = composeFollowTrace(prev, trace)
	}
	rtReq := req.Clone(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := lease.opaque.RoundTrip(rtReq)
	if err != nil {
		if bound {
			lease.inFlight.Store(false)
			lease.markDead()
			return nil, followFatal(ErrFollowConnUnavailable, err)
		}
		return nil, err
	}
	if bound && mismatch.Load() {
		closeResponse(resp)
		lease.inFlight.Store(false)
		lease.markDead()
		return nil, followFatal(ErrFollowTargetChanged, nil)
	}
	return finishResponse(resp, inner, lease, clearInner), nil
}

// composeFollowTrace chains the follow-scope hooks behind every hook already
// present on the context, so an Options.Trace client trace keeps firing.
func composeFollowTrace(prev, added *httptrace.ClientTrace) *httptrace.ClientTrace {
	composed := *prev
	if prev.GotConn != nil || added.GotConn != nil {
		prevHook := prev.GotConn
		addedHook := added.GotConn
		composed.GotConn = func(info httptrace.GotConnInfo) {
			if prevHook != nil {
				prevHook(info)
			}
			if addedHook != nil {
				addedHook(info)
			}
		}
	}
	if prev.PutIdleConn != nil || added.PutIdleConn != nil {
		prevHook := prev.PutIdleConn
		addedHook := added.PutIdleConn
		composed.PutIdleConn = func(err error) {
			if prevHook != nil {
				prevHook(err)
			}
			if addedHook != nil {
				addedHook(err)
			}
		}
	}
	return &composed
}

// followBody releases the scope/lease reservation when the response body
// closes. Closing twice must not untrack a later request sharing the lease.
type followBody struct {
	io.ReadCloser
	once    sync.Once
	onClose func()
}

func (b *followBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.onClose)
	return err
}

// errFollowNoNewDial is returned when the pinned transport would have to
// establish another connection (the bound one was closed, evicted or is
// saturated). It is translated to ErrFollowConnUnavailable for callers.
var errFollowNoNewDial = errors.New("follow scope: opening another connection is not allowed")

// followConn observes the lifecycle of the pinned connection at the dial
// layer: when the transport closes it (idle timeout, server close, protocol
// failure) the lease is marked dead so the next scoped operation fails
// immediately.
type followConn struct {
	net.Conn
	lease *followLease
	once  sync.Once
}

func (c *followConn) Close() error {
	c.once.Do(c.lease.markDead)
	return c.Conn.Close()
}

// newPinnedTransport clones the connection-affecting configuration of the
// client transport into a transport whose pool can ever hold one
// connection. Its dial functions delegate to the originals exactly once;
// any later dial attempt (which means the first connection is gone) fails
// instead of silently opening a new one.
func newPinnedTransport(base *http.Transport) (*http.Transport, *followLease) {
	tr := base.Clone()

	// Clone shares upgrade funcs when the caller configured TLSNextProto by
	// hand; re-establish an isolated HTTP/2 stack instead of registering
	// pinned connections into the base pool.
	if base.TLSNextProto != nil {
		if _, ok := base.TLSNextProto["h2"]; ok {
			tr.TLSNextProto = nil
			tr.ForceAttemptHTTP2 = true
		}
	}
	tr.DisableKeepAlives = false
	if tr.MaxIdleConnsPerHost < 1 {
		tr.MaxIdleConnsPerHost = 1
	}

	lease := &followLease{tr: tr}
	var dialed atomic.Bool

	guardDial := func(dial func(ctx context.Context, network, addr string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			if !dialed.CompareAndSwap(false, true) {
				lease.markDead()
				return nil, errFollowNoNewDial
			}
			conn, err := dial(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &followConn{Conn: conn, lease: lease}, nil
		}
	}

	switch {
	case tr.DialContext != nil:
		tr.DialContext = guardDial(tr.DialContext)
	case tr.Dial != nil:
		legacyDial := tr.Dial
		tr.DialContext = guardDial(func(_ context.Context, network, addr string) (net.Conn, error) {
			return legacyDial(network, addr)
		})
	default:
		dialer := new(net.Dialer)
		tr.DialContext = guardDial(dialer.DialContext)
	}

	// Custom TLS dialers bypass DialContext; guard them as well so a
	// reconnect can never slip past the pin.
	if tr.DialTLSContext != nil {
		orig := tr.DialTLSContext
		tr.DialTLSContext = guardDial(orig)
	} else if tr.DialTLS != nil {
		orig := tr.DialTLS
		tr.DialTLSContext = guardDial(func(_ context.Context, network, addr string) (net.Conn, error) {
			return orig(network, addr)
		})
	}

	return tr, lease
}
