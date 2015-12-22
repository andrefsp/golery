package golery

import (
	"testing"
)

func TestCanRouteMessage(t *testing.T) {

	var receivedMessages = []string{}

	var fn = func(message string) {
		receivedMessages = append(receivedMessages, message)
	}

	var route = Route{queueName: "TestQueue", fn: fn}

	route.fn("this is a test")
	if len(receivedMessages) < 1 {
		t.Fail()
	}
}

func TestGetConfigCreatesRoutes(t *testing.T) {
	var receivedMessages = []string{}
	var function1 = func(message string) {
		receivedMessages = append(receivedMessages, message)
	}

	var routes = []Route{
		Route{queueName: "queue1", fn: function1},
	}
	var config = GetConfig(routes)

	config.routeMap["queue1"].fn("this is a message")

	if len(receivedMessages) < 1 {
		t.Fail()
	}
}
