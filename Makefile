NAME := logrus_clickhouse
GO := go

all: get test build
build:
	$(GO) build .

test:
	$(GO) test .

get:
	$(GO) get -d .
