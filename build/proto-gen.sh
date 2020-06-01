#!/bin/bash

protoc -I . --go_out=plugins=grpc:. ./internal/proto-files/raft.proto