package okq

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randString() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func TestClient(t *T) {
	c := New("localhost:4777")
	q := randString()
	assert := assert.New(t)
	require := require.New(t)

	// Queue is currently empty, make sure PeekNext and PeekLast return nil
	e, err := c.PeekNext(q)
	require.Nil(err)
	assert.Nil(e)

	e, err = c.PeekLast(q)
	require.Nil(err)
	assert.Nil(e)

	// Add some items to the queue
	require.Nil(c.Push(q, "foo"))
	require.Nil(c.Push(q, "bar"))
	require.Nil(c.PushHigh(q, "baz"))

	// The queue should now be (from first to last) "baz", "foo", "bar"
	e, err = c.PeekNext(q)
	require.Nil(err)
	assert.Equal(e.Contents, "baz")

	e, err = c.PeekLast(q)
	require.Nil(err)
	assert.Equal(e.Contents, "bar")

	status, err := c.Status(q)
	require.Nil(err)
	assert.Equal(status[0], fmt.Sprintf("%s total: 3 processing: 0 consumers: 0", q))
}

func TestConsumer(t *T) {
	c1, c2 := New("localhost:4777"), New("localhost:4777")
	c1.Timeout = 2 * time.Second
	q := randString()
	assert := assert.New(t)
	require := require.New(t)

	stopCh := make(chan bool)
	workCh := make(chan bool)

	i := 0
	fn := func(eq string, e *Event) bool {
		assert.Equal(q, eq)
		assert.Equal(strconv.Itoa(i), e.Contents)
		i++
		workCh <- true
		return true
	}

	retCh := make(chan error)
	go func() {
		retCh <- c1.Consumer(fn, stopCh, q)
	}()

	for i := 0; i < 1000; i++ {
		require.Nil(c2.Push(q, strconv.Itoa(i)))
		<-workCh
	}

	close(stopCh)
	require.Nil(<-retCh)
	require.Nil(c1.Close())

	status, err := c2.Status(q)
	require.Nil(err)
	assert.Equal(fmt.Sprintf("%s total: 0 processing: 0 consumers: 0", q), status[0])
}
