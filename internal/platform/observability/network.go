package observability

import (
	"context"
	"net"
)

type countedConn struct {
	net.Conn
	registry *Registry
	prefix   string
}

func (c countedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.registry.Add(c.prefix+"_wire_read_bytes", int64(n))
	return n, err
}
func (c countedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.registry.Add(c.prefix+"_wire_write_bytes", int64(n))
	return n, err
}

// CountDial measures actual protocol byte streams; TCP/IP framing is excluded.
func CountDial(dial func(context.Context, string, string) (net.Conn, error), r *Registry, prefix string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return c, nil
		}
		r.Add(prefix+"_connections", 1)
		return countedConn{c, r, prefix}, nil
	}
}
