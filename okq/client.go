// Package okq is a go client for the okq persitent queue
//
// To import inside your package do:
//
//	import "github.com/mediocregopher/okq-go/okq"
//
// Connecting
//
// Use New to create a Client. This Client can have knowledge of multiple okq
// endpoints, and will attempt to reconnect at random if it loses connection. In
// most cases it will only return an error if it can't connect to any of the
// endpoints at that moment.
//
//	cl := okq.New("127.0.0.1:4777", "127.0.0.1:4778")
//
// Pushing to queues
//
// All events in okq require a unique event id. This package will automatically
// generate a unique id if you use the standard Push methods.
//
//	cl.Push("super-queue", "my awesome event", okq.Normal)
//
// You can also create your own id by using the PushEvent methods. Remember
// though that the event id *must* be unique within that queue.
//
//	e := okq.Event{"super-queue", "unique id", "my awesome event"}
//	cl.PushEvent(&e, okq.Normal)
//
// Consuming from queues
//
// You can turn any Client into a consumer by using the Consumer methods. These
// will block as they call the given function on incoming events, and only
// return upon an error or a manual stop.
//
// Example of a consumer which should never quit
//	fn := func(e *okq.Event) bool {
//		log.Printf("event received on %s: %s", e.Queue, e.Contents)
//		return true
//	}
//	for {
//		err := cl.Consumer(fn, nil, "queue1", "queue2")
//		log.Printf("error received from consumer: %s", err)
//	}
//
// See the doc string for the Consumer method for more details
package okq

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/grooveshark/golib/agg"
	"github.com/mediocregopher/radix.v2/redis"
)

var uuidCh = make(chan string, 1024)

func init() {
	go func() {
		for {
			uuidCh <- uuid.New()
		}
	}()
}

// PushFlag is passed into either of the Push commands to alter their behavior.
// You can or multiple of these together to combine their behavior
type PushFlag int

const (
	// Normal is the expected behavior (call waits for event to be committed to
	// okq, normal priority)
	Normal PushFlag = 1 << iota

	// HighPriority causes the pushed event to be placed at the front of the
	// queue instead of the back
	HighPriority

	// NoBlock causes set the server to not wait for the event to be committed
	// to disk before replying, it will reply as soon as it can and commit
	// asynchronously
	NoBlock
)

// DefaultTimeout is used when reading from socket
const DefaultTimeout = 30 * time.Second

// Notify timeout used in the consumer
const notifyTimeout = time.Duration(float64(DefaultTimeout) * 0.9)

// If true turns on debug logging and agg support (see
// https://github.com/grooveshark/golib)
var Debug bool

// Event is a single event which can be read from or written to an okq instance
type Event struct {
	Queue    string // The queue the event is coming from/going to
	ID       string // Unique id of this event
	Contents string // Arbitrary contents of the event
}

func replyToEvent(q string, r *redis.Resp) (*Event, error) {
	if r.IsType(redis.Nil) {
		return nil, nil
	}
	parts, err := r.List()
	if err != nil {
		return nil, err
	} else if len(parts) < 2 {
		return nil, errors.New("not enough elements in reply")
	}
	return &Event{
		Queue:    q,
		ID:       parts[0],
		Contents: parts[1],
	}, nil
}

// Client is a client for the okq persistant queue. It can talk to a pool of okq
// instances and failover from one to the other if one loses connectivity
type Client struct {
	clients map[string]*redis.Client

	// Timeout to use for reads/writes to okq. This defaults to DefaultTimeout,
	// but can be overwritten immediately after NewClient is called
	Timeout time.Duration
}

// New takes one or more okq endpoints (all in the same pool) and returns a
// client which will interact with them. Returns an error if it can't connect to
// any of the given clients
func New(addr ...string) *Client {
	c := Client{
		clients: map[string]*redis.Client{},
		Timeout: DefaultTimeout,
	}

	for i := range addr {
		c.clients[addr[i]] = nil
	}

	return &c
}

func (c *Client) getConn() (string, *redis.Client, error) {
	for addr, rclient := range c.clients {
		if rclient != nil {
			return addr, rclient, nil
		}
	}

	for addr := range c.clients {
		rclient, err := redis.DialTimeout("tcp", addr, c.Timeout)
		if err == nil {
			c.clients[addr] = rclient
			return addr, rclient, nil
		}
	}

	return "", nil, errors.New("no connectable endpoints")
}

func doCmd(
	rclient *redis.Client, cmd string, args ...interface{},
) *redis.Resp {
	start := time.Now()
	r := rclient.Cmd(cmd, args...)
	if Debug && r.Err == nil {
		agg.Agg(strings.ToUpper(cmd), time.Since(start).Seconds())
	}
	return r
}

func (c *Client) cmd(cmd string, args ...interface{}) *redis.Resp {
	for i := 0; i < 3; i++ {
		addr, rclient, err := c.getConn()
		if err != nil {
			return redis.NewResp(err)
		}

		r := doCmd(rclient, cmd, args...)
		if err := r.Err; err != nil {
			if r.IsType(redis.IOErr) {
				rclient.Close()
				c.clients[addr] = nil
				continue
			}
		}

		return r
	}

	return redis.NewResp(errors.New("could not find usable endpoint"))
}

// PeekNext returns the next event which will be retrieved from the queue,
// without actually removing it from the queue. Returns nil if the queue is
// empty
func (c *Client) PeekNext(queue string) (*Event, error) {
	return replyToEvent(queue, c.cmd("QRPEEK", queue))
}

// PeekLast returns the event most recently added to the queue, without actually
// removing it from the queue. Returns nil if the queue is empty
func (c *Client) PeekLast(queue string) (*Event, error) {
	return replyToEvent(queue, c.cmd("QLPEEK", queue))
}

// PushEvent pushes the given event onto its queue. The event's Id must be
// unique within that queue
func (c *Client) PushEvent(e *Event, f PushFlag) error {
	cmd := "QLPUSH"
	if f&HighPriority > 0 {
		cmd = "QRPUSH"
	}
	args := append(make([]interface{}, 0, 4), e.Queue, e.ID, e.Contents)
	if f&NoBlock > 0 {
		args = append(args, "NOBLOCK")
	}

	return c.cmd(cmd, args...).Err
}

// Push pushes an event with the given contents onto the queue. The event's ID
// will be an automatically generated uuid
//
// Normal event:
//
//	cl.Push("queue", "some event", okq.Normal)
//
// High priority event:
//
//	cl.Push("queue", "some important event", okq.HighPriority)
//
// Submit an event as fast as possible
//
//	cl.Push("queue", "not that important event", okq.NoBlock)
//
func (c *Client) Push(queue, contents string, f PushFlag) error {
	event := Event{Queue: queue, ID: <-uuidCh, Contents: contents}
	return c.PushEvent(&event, f)
}

// QueueStatus describes the current status for a single queue, as described by
// the QSTATUS command
type QueueStatus struct {
	Name       string // Name of the queue
	Total      int64  // Total events in the queue, includes ones being processed
	Processing int64  // Number of events currently being processed
	Consumers  int64  // Number of connections registered as consumers for this queue
}

// Status returns the statuses of the given queues, or the statuses of all the
// known queues if no queues are given
func (c *Client) Status(queue ...string) ([]QueueStatus, error) {
	arr, err := c.cmd("QSTATUS", queue).Array()
	if err != nil {
		return nil, err
	}
	statuses := make([]QueueStatus, len(arr))
	for i := range arr {
		status, err := arr[i].Array()
		if err != nil {
			return nil, err
		} else if len(status) < 4 {
			return nil, fmt.Errorf("not enough elements in status: %s", status)
		}
		name, err := status[0].Str()
		if err != nil {
			return nil, err
		}
		total, err := status[1].Int64()
		if err != nil {
			return nil, err
		}
		processing, err := status[2].Int64()
		if err != nil {
			return nil, err
		}
		consumers, err := status[3].Int64()
		if err != nil {
			return nil, err
		}
		statuses[i] = QueueStatus{
			Name:       name,
			Total:      total,
			Processing: processing,
			Consumers:  consumers,
		}
	}
	return statuses, nil
}

// Close closes all connections that this client currently has open
func (c *Client) Close() error {
	var err error
	for addr, rclient := range c.clients {
		rerr := rclient.Close()
		if err == nil && rerr != nil {
			err = rerr
		}
		c.clients[addr] = nil
	}
	return err
}

// ConsumerFunc is passed into Consume, and is used as a callback for incoming
// Events. It should return true if the event was processed successfully and
// false otherwise. If ConsumerUnsafe is being used the return is ignored
type ConsumerFunc func(*Event) bool

// Consumer turns a client into a consumer. It will register itself on the given
// queues, and call the ConsumerFunc on all events it comes across. If stopCh is
// non-nil and is closed this will return immediately (unless blocking on a
// QNOTIFY command, in which case it will return after that returns).
//
// The ConsumerFunc is called synchronously, so if you wish to process events in
// parallel you'll have to create multiple connections to okq
func (c *Client) Consumer(
	fn ConsumerFunc, stopCh chan bool, queues ...string,
) error {
	return c.consumer(fn, stopCh, queues, false)
}

// ConsumerUnsafe is the same as Consumer except that the given ConsumerFunc is
// called asynchronously and its return value doesn't matter (because no QACK is
// ever sent to the okq server)
func (c *Client) ConsumerUnsafe(
	fn ConsumerFunc, stopCh chan bool, queues ...string,
) error {
	return c.consumer(fn, stopCh, queues, true)
}

func (c *Client) consumer(
	fn ConsumerFunc, stopCh chan bool, queues []string, noack bool,
) error {
	if len(queues) == 0 {
		return errors.New("no queues given to read from")
	}

	addr, rclient, err := c.getConn()
	if err != nil {
		return err
	}

	// If we're returning and stopCh isn't closed it means there was some kind
	// of connection error, and we should close the client.  It's possible that
	// stopCh was closed AND there was a connection error, in that case the
	// faulty connection will stay in c.clients, but it will be ferreted out the
	// next time it tries to get used
	defer func() {
		select {
		case <-stopCh:
		default:
			rclient.Close()
			c.clients[addr] = nil
		}
	}()

	if err := doCmd(rclient, "QREGISTER", queues).Err; err != nil {
		return err
	}

	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		r := doCmd(rclient, "QNOTIFY", int(notifyTimeout.Seconds()))
		if err := r.Err; err != nil {
			return err
		}

		if r.IsType(redis.Nil) {
			continue
		}

		q, err := r.Str()
		if err != nil {
			return err
		}

		args := []string{q}
		if noack {
			// okq uses EX 0 to indicate that no qack is needed now, but it used
			// to use NOACK. We send both for now for backwards compatibility.
			// One day this may get in the way, so we'll have to take it out
			args = append(args, "EX", "0")
			args = append(args, "NOACK")
		}

		e, err := replyToEvent(q, doCmd(rclient, "QRPOP", args))
		if err != nil {
			return err
		} else if e == nil {
			continue
		}

		if noack {
			go fn(e)
			continue
		}

		if fn(e) {
			doCmd(rclient, "QACK", q, e.ID)
		}
	}
}
