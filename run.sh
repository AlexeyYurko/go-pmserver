#!/bin/bash

export pmserver=development
export pmserver_local_run=local
#export GIN_MODE=release

go build
./go-pmserver
