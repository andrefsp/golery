package golery

import (
	"fmt"
	"github.com/streadway/amqp"
	"os"
)

type Route struct {
	queueName string
	fn        func([]byte)
}

type Config struct {
	RABBITMQ_URL string
	routeMap     map[string]Route
}

func GetConfig(routes []Route) Config {

	var RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
	var routeMap = make(map[string]Route)

	if os.Getenv("RABBITMQ_URL") != "" {
		RABBITMQ_URL = os.Getenv("RABBITMQ_URL")
	}

	for i := 0; i < len(routes); i++ {
		routeMap[routes[i].queueName] = routes[i]
	}

	return Config{RABBITMQ_URL: RABBITMQ_URL, routeMap: routeMap}
}

func StartQueueConsumer(queueName string, config Config) {
	var connection, err = amqp.Dial(config.RABBITMQ_URL)

	if err != nil {
		fmt.Println("connection.open: %s", err)
	}
	defer connection.Close()

	c, err := connection.Channel()

	if err != nil {
		fmt.Println("channel.open: %s", err)
	}

	messages, err := c.Consume(queueName, queueName, true, false, false, false, nil)
	if err != nil {
		fmt.Println("basic.consume: %v", err)
	}

	for message := range messages {
		config.routeMap[queueName].fn(message.Body)
	}
}

func Start(routes []Route) {
	var config = GetConfig(routes)
	for queueName, _ := range config.routeMap {
		go StartQueueConsumer(queueName, config)
	}
}
