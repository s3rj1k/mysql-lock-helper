#!/bin/sh

# env
export GOPATH=${HOME}/.go
export PATH=$PATH:$GOPATH/bin

go build -a
