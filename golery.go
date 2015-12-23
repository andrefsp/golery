package golery

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"strconv"
)

type Route struct {
	queueName string
	fn        func([]byte)
	workers   int
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

func createQueue(channel *amqp.Channel, queueName string) {

	err := channel.ExchangeDeclare(
		queueName, // name of the exchange
		"direct",  // type
		true,      // durable
		false,     // delete when complete
		false,     // internal
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		log.Fatal("Couldn't declare exchange: %s", err)
	}

	queue, err := channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)

	if err != nil {
		log.Fatal("Couldn't declare queue: %s", err)
	}

	err = channel.QueueBind(
		queue.Name, // name of the queue
		queueName,  // bindingKey
		queueName,  // sourceExchange
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		log.Fatal("Couldn't bind queue: %s", err)
	}

}

func messageConsumerWorker(messageChannel chan []byte, fn func([]byte), workerName string) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recover from", r)
		}
	}()

	for message := range messageChannel {
		log.Println("Message received on worker ", workerName)
		fn(message)
	}
}

func StartQueueConsumer(queueName string, config Config, TerminatedConsumerChannel chan<- bool) {
	var connection, err = amqp.Dial(config.RABBITMQ_URL)

	if err != nil {
		log.Fatal("connection.open: %s", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal("channel.open: %s", err)
	}

	createQueue(channel, queueName)

	messages, err := channel.Consume(queueName, queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatal("basic.consume: %v", err)
	}

	var messageChannel = make(chan []byte)

	var workers = 1

	if config.routeMap[queueName].workers > 0 {
		workers = config.routeMap[queueName].workers
	}

	// Start the workers
	for i := 0; i < workers; i++ {
		workerName := queueName + "." + strconv.Itoa(i)
		go messageConsumerWorker(messageChannel, config.routeMap[queueName].fn, workerName)
	}

	// Push messages into channel
	for message := range messages {
		messageChannel <- message.Body
	}

	TerminatedConsumerChannel <- true
}

func Start(routes []Route) {
	var config = GetConfig(routes)
	var TerminatedConsumerChannel = make(chan bool, len(routes))

	for queueName, _ := range config.routeMap {
		go StartQueueConsumer(queueName, config, TerminatedConsumerChannel)
	}

	for i := 0; i < len(routes); i++ {
		<-TerminatedConsumerChannel
	}
}
