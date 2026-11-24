package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestArchiveRetriesWhenCommitResponseIsLost(t *testing.T) {
	for _, err := range []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed},
	} {
		if !retryableArchiveError(fmt.Errorf("archive commit: %w", err)) {
			t.Errorf("lost response must reload durable PostgreSQL position: %v", err)
		}
	}
	if retryableArchiveError(errors.New("stale archive owner or offset")) {
		t.Fatal("an ownership violation must not enter the network retry loop")
	}
}
