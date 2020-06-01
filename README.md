# RAFT-GRPC

A simple Raft implementation using gRPC for communication.

[Raft whitepaper](https://raft.github.io/raft.pdf)

## Pre-requisites

1. Use go v1.14+
2. Download the necessary dependencies using the following command

```shell
$ go mod download
```

## Usage

To start the first Node

```shell
$ go run ./cmd/main.go -id=1 -addr=localhost:20001
```

To start a peer which connect to the first Node

```shell
$ go run ./cmd/main.go -id=2 -addr=localhost:20002 -join=localhost:20001
```

## Future

1. Extend it to be a distributed key-value store
