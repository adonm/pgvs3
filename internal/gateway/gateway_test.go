package gateway

import (
	"io"
	"net"
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
