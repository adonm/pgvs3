package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestLimitListenerConnectionCapLivesUntilClose(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := newLimitListener(base, 1)
	t.Cleanup(func() { _ = ln.Close() })

	first, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	defer conn.Close()
	_ = first.Close()

	second, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	_, readErr := second.Read(make([]byte, 1))
	_ = second.Close()
	if readErr == nil {
		t.Fatal("second connection stayed open while the only slot was live")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}

	third, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	_, _ = io.WriteString(third, "GET / HTTP/1.1\r\nHost: test\r\n\r\n")

	accepted := make(chan net.Conn, 1)
	errs := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errs <- err
			return
		}
		accepted <- c
	}()
	select {
	case c := <-accepted:
		_ = c.Close()
	case err := <-errs:
		t.Fatalf("accept after release: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("connection slot was not released")
	}
}

func TestRunHTTPCancelsRequestContextAndShutsDownPromptly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	inHandler := make(chan struct{})
	handlerSawCancel := make(chan struct{})
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(inHandler)
			<-r.Context().Done()
			close(handlerSawCancel)
			w.WriteHeader(http.StatusServiceUnavailable)
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runHTTPListener(ctx, ln, srv, 500*time.Millisecond, 16, nil)
	}()

	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		resp, err := http.Get("http://" + ln.Addr().String() + "/test")
		if err == nil {
			resp.Body.Close()
		}
	}()

	<-inHandler
	t0 := time.Now()
	cancel()

	select {
	case <-handlerSawCancel:
	case <-time.After(time.Second):
		t.Fatal("in-flight request context was not cancelled on shutdown")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runHTTPListener = %v", err)
		}
		if time.Since(t0) > time.Second {
			t.Fatalf("shutdown took %v, expected prompt exit", time.Since(t0))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runHTTPListener did not shut down promptly")
	}
	<-reqDone
}
