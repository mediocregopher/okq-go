# okq-go

A go driver for the [okq](https://github.com/mc0/okq) persistent queue.

okq uses the redis protocol and calling conventions as it's interface, so any
standard redis client can be used to interact with it. This package wraps around
a normal redis driver, however, to provide a convenient interface to interfact
with. Specifically, it creates a simple interface for event consumers to use so
they retrieve events through a channel rather than implementing that logic
manually everytime.

## Usage

To get:

    go get github.com/mediocregopher/okq-go/okq

To import:

    import "github.com/mediocregopher/okq-go/okq"

API docs can be found [here](http://godoc.org/github.com/mediocregopher/okq-go/okq)
