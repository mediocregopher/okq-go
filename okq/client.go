// Go client package for the okq persitent queue
//
// TO import inside your package do:
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
//	cl.Push("super-queue", "my awesome event")
//
// You can also create your own id by using the PushEvent methods. Remember
// though that the event id *must* be unique within that queue.
//
//	cl.PushEvent("super-queue", &okq.Event{"unique id", "my awesome event"})
//
// Consuming from queues
//
// You can turn any Client into a consumer by using the Consumer methods. These
// will block as they write incoming events to a given channel, and only return
// upon an error or a manual stop.
//
//	// Example of a consumer which should never quit
//	ch := make(chan *okq.ConsumerEvent)
//	go func() {
//		for e := range ch {
//			log.Printf("event received: %v", e.Event.Contents)
//			e.Ack()
//		}
//	}()
//
//	for {
//		err := cl.Consumer(ch, nil, "queue1", "queue2")
//		log.Printf("error received from consumer: %s", err)
//	}
//
// See the doc string for the Consumer method for more details
package okq

import (
	"errors"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/fzzy/radix/redis"
)

var uuidCh = make(chan string, 1024)
func init() {
	go func() {
		for {
			uuidCh <- uuid.New()
		}
	}()
}

// Timeout to use when reading from socket
const TIMEOUT = 30 * time.Second

// Notify timeout used in the consumer
const notify_timeout = TIMEOUT - (1 * time.Second)

// A single event which can be read from or written to an okq instance
type Event struct {
	Id       string // Unique id of this event
	Contents string // Arbitrary contents of the event
}

func replyToEvent(r *redis.Reply) (*Event, error) {
	if r.Type == redis.NilReply {
		return nil, nil
	}
	parts, err := r.List()
	if err != nil {
		return nil, err
	} else if len(parts) < 2 {
		return nil, errors.New("not enough elements in reply")
	}
	return &Event{
		Id:       parts[0],
		Contents: parts[1],
	}, nil
}

type Client struct {
	clients map[string]*redis.Client

	// Timeout to use for reads/writes to okq. This defaults to TIMEOUT, but can
	// be overwritten immediately after NewClient is called
	Timeout time.Duration
}

// Given one or more okq endpoints (all in the same pool), returns a client
// which will interact with them. Returns an error if it can't connect to any of
// the given clients
func New(addr ...string) *Client {
	c := Client{
		clients: map[string]*redis.Client{},
		Timeout: TIMEOUT,
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

func (c *Client) cmd(cmd string, args ...interface{}) *redis.Reply {
	for i := 0; i < 3; i++ {
		addr, rclient, err := c.getConn()
		if err != nil {
			return &redis.Reply{Type: redis.ErrorReply, Err: err}
		}

		r := rclient.Cmd(cmd, args...)
		if err := r.Err; err != nil {
			if _, ok := err.(*redis.CmdError); !ok {
				rclient.Close()
				c.clients[addr] = nil
				continue
			}
		}

		return r
	}

	return &redis.Reply{
		Type: redis.ErrorReply,
		Err:  errors.New("could not find usable endpoint"),
	}
}

// Returns the next event which will be retrieved from the queue, without
// actually removing it from the queue. Returns nil if the queue is empty
func (c *Client) PeekNext(queue string) (*Event, error) {
	return replyToEvent(c.cmd("QRPEEK", queue))
}

// Returns the event most recently added to the queue, without actually removing
// it from the queue. Returns nil if the queue is empty
func (c *Client) PeekLast(queue string) (*Event, error) {
	return replyToEvent(c.cmd("QLPEEK", queue))
}

// Pushes the given event onto the end of the queue. The event's Id must be
// unique within that queue
func (c *Client) PushEvent(queue string, event *Event) error {
	return c.cmd("QLPUSH", queue, event.Id, event.Contents).Err
}

// Pushes an event with the given contents onto the end of the queue. The
// event's Id will be an automatically generated uuid
func (c *Client) Push(queue, contents string) error {
	event := Event{Id: <-uuidCh, Contents: contents}
	return c.PushEvent(queue, &event)
}

// Pushes the given event onto the front of the queue (meaning it will be the
// next event consumed). The event's Id must be unique within that queue
func (c *Client) PushEventHigh(queue string, event *Event) error {
	return c.cmd("QRPUSH", queue, event.Id, event.Contents).Err
}

// Pushes an event with the given contents onto the end of the queue. The
// event's Id will be an automatically generated uuid
func (c *Client) PushHigh(queue, contents string) error {
	event := Event{Id: <-uuidCh, Contents: contents}
	return c.PushEventHigh(queue, &event)
}

// Returns the statuses of the given queues, or the statuses of all the known
// queues if no queues are given
func (c *Client) Status(queue ...string) ([]string, error) {
	return c.cmd("QSTATUS", queue).List()
}

// Closes all connections that this client currently has open
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

type ack struct {
	id, queue string
}

// An event as returned by a consumer client. It contains an Event, but it must
// have Ack() called on it (unless ConsumerUnsafe is used)
type ConsumerEvent struct {
	*Event
	Queue  string
	ackCh  chan *ack
	stopCh chan bool
}

// Acknowledges that the given ConsumerEvent has been successfully consumed. If
// this is not called by the event's timeout the event will be put back in its
// queue to be consumed again
func (we *ConsumerEvent) Ack() {
	select {
	case we.ackCh <- &ack{we.Event.Id, we.Queue}:
	case <-we.stopCh:
	}
}

// Turns this client into a consumer. It will register itself on the given
// queues, and push all incoming events to the given ConsumerEvent channel. If
// stopCh is not nil it can be closed in order to stop the consumer. ch will be
// closed when stopCh is closed. Note that some events may not be successfully
// ack'd if stopCh is closed before they have Ack() called on them.
//
// This call will block until the first connection error, which will be
// returned. The method may be called again after this, but you may keep getting
// back connection errors if none of the endpoints are reachable.
//
// Since this call blocks indefinitely it's necessary to be reading off of ch in
// a separate goroutine from the one calling this method.
func (c *Client) Consumer(
	ch chan *ConsumerEvent, stopCh chan bool, queue ...string,
) error {
	return c.consumer(ch, stopCh, queue, false)
}

// Same as Consumer, but it's not necessary to call Ack on events received from
// ch. This is more unsafe as if the consumer dies while processing the event
// the event is lost forever
func (c *Client) ConsumerUnsafe(
	ch chan *ConsumerEvent, stopCh chan bool, queue ...string,
) error {
	return c.consumer(ch, stopCh, queue, true)
}

func (c *Client) consumer(
	ch chan *ConsumerEvent, stopCh chan bool, queues []string, noack bool,
) error {

	if len(queues) == 0 {
		return errors.New("no queues given to read from")
	}

	addr, rclient, err := c.getConn()
	if err != nil {
		return err
	}

	// If the stopCh is EVER closed we want to make sure the  ch gets
	// closed also. If we're returning and stopCh isn't closed it means
	// there was some kind of connection error, and we should close the client.
	// It's possible that stopCh was closed AND there was a connection error, in
	// that case the faulty connection will stay in c.clients, but it will be
	// ferreted out the next time it tries to get used
	defer func() {
		select {
		case <-stopCh:
			close(ch)
		default:
			rclient.Close()
			c.clients[addr] = nil
		}
	}()

	if err := rclient.Cmd("QREGISTER", queues).Err; err != nil {
		return err
	}

	notifyTimeout := (c.Timeout - (1 * time.Second)).Seconds()
	ackCh := make(chan *ack)
	ackErrCh := make(chan error)
	go ackHandler(addr, c.Timeout, ackCh, ackErrCh)
	defer close(ackCh)
	for {
		r := rclient.Cmd("QNOTIFY", notifyTimeout)
		if err := r.Err; err != nil {
			return err
		}

		// Check if we've stopped or errord before doing anything else. We may
		// loop back to the top in the step after this so it's important to do
		// this now
		select {
		case err := <-ackErrCh:
			return err
		case <-stopCh:
			return nil
		default:
		}

		if r.Type == redis.NilReply {
			continue
		}

		q, err := r.Str()
		if err != nil {
			return err
		}

		args := []string{q}
		if noack {
			args = append(args, "NOACK")
		}

		e, err := replyToEvent(rclient.Cmd("QRPOP", args))
		if err != nil {
			return err
		} else if e == nil {
			continue
		}

		select {
		case ch <- &ConsumerEvent{e, q, ackCh, stopCh}:
		case err := <-ackErrCh:
			return err
		case <-stopCh:
			return nil
		}
	}
}

func ackHandler(
	addr string, timeout time.Duration, ackCh chan *ack, ackErrCh chan error,
) {
	rclient, err := redis.DialTimeout("tcp", addr, timeout)
	if err != nil {
		ackErrCh <- err
		return
	}
	defer rclient.Close()

	for a := range ackCh {
		if err := rclient.Cmd("QACK", a.queue, a.id).Err; err != nil {
			ackErrCh <- err
			return
		}
	}
}
