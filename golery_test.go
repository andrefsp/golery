package golery

import (
	"testing"
)

func TestCanRouteMessage(t *testing.T) {
	var receivedMessages = [][]byte{}
	var fn = func(message []byte) {
		receivedMessages = append(receivedMessages, message)
	}

	var route = Route{queueName: "TestQueue", fn: fn}

	route.fn([]byte("this is a test"))
	if len(receivedMessages) < 1 {
		t.Fail()
	}
}

func TestGetConfigCreatesRoutes(t *testing.T) {
	var receivedMessages = [][]byte{}
	var function1 = func(message []byte) {
		receivedMessages = append(receivedMessages, message)
	}

	var routes = []Route{
		Route{queueName: "queue1", fn: function1},
	}
	var config = GetConfig(routes)

	config.routeMap["queue1"].fn([]byte("this is a message"))

	if len(receivedMessages) < 1 {
		t.Fail()
	}
}

func TestCanIterateConfig(t *testing.T) {
	var receivedMessages = [][]byte{}
	var function1 = func(message []byte) {
		receivedMessages = append(receivedMessages, message)
	}

	var config = GetConfig([]Route{
		Route{queueName: "queue1", fn: function1},
	})

	for key, route := range config.routeMap {
		route.fn([]byte(key))
	}

	if len(receivedMessages) < 1 {
		t.Fail()
	}
}
