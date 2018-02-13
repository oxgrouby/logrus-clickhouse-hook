package hook

import (
	"testing"
	"github.com/Sirupsen/logrus"
	"fmt"
	"time"
	"io/ioutil"
)

const (
	clickHouseHost = "clickhouse-server"
)

func TestHook(t *testing.T) {

	log := logrus.New()
	log.Out = ioutil.Discard

	clickhouse := makeClickHouseConfig()

	hook, err := NewHook(clickhouse)
	if err != nil {
		t.Errorf("Error when initialization hook: %s", err)
	}

	if err == nil {
		log.AddHook(hook)
	}

	for i := 0; i < 100; i++ {
		currentTime := time.Now()

		log.WithFields(logrus.Fields{
			"remote_addr":            fmt.Sprint("sync_test", i),
			"upstream_addr":          fmt.Sprint("sync_test", i),
			"request_time":           fmt.Sprint("sync_test", i),
			"upstream_response_time": fmt.Sprint("sync_test", i),
			"request":                fmt.Sprint("sync_test", i),
			"status":                 fmt.Sprint("sync_test", i),
			"bytes_sent":             i,
			"time":                   currentTime.Unix(),
			"event_date":             currentTime.Format("2006-01-02"),
		}).Info("Sync message for clickhouse")
	}

}

func TestAsyncHook(t *testing.T) {

	log := logrus.New()
	log.Out = ioutil.Discard

	clickhouse := makeClickHouseConfig()

	hook, err := NewAsyncHook(clickhouse)
	if err != nil {
		t.Errorf("Error when initialization hook: %s", err)
	}

	if err == nil {
		log.AddHook(hook)
	}

	for i := 0; i < 100000; i++ {
		currentTime := time.Now()

		log.WithFields(logrus.Fields{
			"remote_addr":            fmt.Sprint("async_test", i),
			"upstream_addr":          fmt.Sprint("async_test", i),
			"request_time":           fmt.Sprint("async_test", i),
			"upstream_response_time": fmt.Sprint("async_test", i),
			"request":                fmt.Sprint("async_test", i),
			"status":                 fmt.Sprint("async_test", i),
			"bytes_sent":             i,
			"time":                   currentTime.Unix(),
			"event_date":             currentTime.Format("2006-01-02"),
		}).Info("Async message for clickhouse")
	}

	hook.Close()
}

func makeClickHouseConfig() *ClickHouse {
	clickhouse := &ClickHouse{
		Db:    "logs",
		Table: "nginx_logs",
		Host:  clickHouseHost,
		Port:  "8123",
		Columns: []string{
			"remote_addr",
			"upstream_addr",
			"request_time",
			"upstream_response_time",
			"request",
			"status",
			"bytes_sent",
			"time",
			"event_date",
		},
		Credentials: struct {
			User     string
			Password string
		}{
			User: "default",
		},
	}
	return clickhouse
}
