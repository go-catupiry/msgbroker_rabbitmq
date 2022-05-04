package msgbroker_rabbitmq

import (
	"fmt"
	"log"
	"time"

	"github.com/go-catupiry/msgbroker"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/go-catupiry/catu"
	"github.com/streadway/amqp"
)

func NewRabbitMQBroker(uri string) *RabbitMQBroker {
	b := RabbitMQBroker{
		URI:               uri,
		RabbitQueues:      make(map[string]*RabbitQueue),
		ReconectionDelay:  5,
		ReconnectionLimit: 10,
	}
	return &b
}

type RabbitMQBroker struct {
	msgbroker.Client
	URI               string
	RabbitQueues      map[string]*RabbitQueue
	Connection        *amqp.Connection
	ReconectionDelay  int
	ReconnectionCount int
	ReconnectionLimit int
	ConnectionError   chan error
}

type RabbitQueue struct {
	Name        string
	Channel     *amqp.Channel
	IsConsuming bool
	Handler     func(queueName string, msg *amqp.Delivery) error
}

func (q *RabbitQueue) Consume(b *RabbitMQBroker) error {
	if q.IsConsuming {
		logrus.WithFields(logrus.Fields{
			"queueName":   q.Name,
			"IsConsuming": q.IsConsuming,
		}).Debug("msgbroker Consume is already consuming")
		return nil
	}

	deliveries, err := q.Channel.Consume(
		q.Name,
		"",
		false, // noAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return err
	}
	q.IsConsuming = true

	go q._Consume(b, deliveries)

	return nil
}

func (q *RabbitQueue) _Consume(b *RabbitMQBroker, deliveries <-chan amqp.Delivery) error {
	for d := range deliveries {
		if q.Handler == nil {
			log.Println("Unknow handler!!!!!!!!!!!!!!!")
			break
		}

		err := q.Handler(q.Name, &d)
		if err != nil {
			d.Nack(false, true)
		} else {
			d.Ack(false)
		}
	}

	return nil
}

func (b *RabbitMQBroker) Init(app catu.App) error {
	return b.Connect()
}

// func (b *RabbitMQBroker) Subscribe(queueName string, handler func(queueName string, msg *amqp.Delivery) error) error {
// 	var err error

// 	logrus.WithFields(logrus.Fields{
// 		"queueName": queueName,
// 	}).Debug("msgbroker Subscribe running")

// 	q, err := b.GetRabbitQueue(queueName)
// 	if err != nil {
// 		return err
// 	}

// 	q.Handler = handler

// 	go q.Consume(b)

// 	return err
// }

// func (b *RabbitMQBroker) Publish(queueName, body string) error {
// 	logrus.WithFields(logrus.Fields{
// 		"queueName": queueName,
// 		"body":      body,
// 	}).Debug("Publish running with")

// 	headers := make(amqp.Table)

// 	q, err := b.GetRabbitQueue(queueName)
// 	if err != nil {
// 		return errors.Wrap(err, "Publish error on get queue")
// 	}

// 	if err = q.Channel.Publish(
// 		queueName, // publish to an exchange
// 		queueName, // routing to 0 or more queues
// 		false,     // mandatory
// 		false,     // immediate
// 		amqp.Publishing{
// 			Headers:         headers,
// 			ContentType:     "text/plain",
// 			ContentEncoding: "",
// 			Body:            []byte(body),
// 			DeliveryMode:    amqp.Persistent,
// 			// Priority:        0, // 0-9
// 			// a bunch of application/implementation-specific fields
// 		},
// 	); err != nil {
// 		return errors.Wrap(err, "Publish error")
// 	}

// 	return nil
// }

func (b *RabbitMQBroker) Connect() error {
	logrus.WithFields(logrus.Fields{}).Debug("msgbroker Connect")

	var err error
	b.Connection, err = amqp.Dial(b.URI)
	if err != nil {
		return errors.Wrap(err, "Error in creating rabbitmq connection")
	}

	go func() {
		// Listen to NotifyClose for reconnections
		<-b.Connection.NotifyClose(make(chan *amqp.Error))
		err := b.Reconnect()
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error": fmt.Sprintf("%+v\n", err),
			}).Error("msgbroker error on reconnect")
		}
	}()

	return nil
}

func (b *RabbitMQBroker) Reconnect() error {
	logrus.WithFields(logrus.Fields{
		"ReconnectionCount": b.ReconnectionCount,
		"ReconnectionLimit": b.ReconnectionLimit,
	}).Debug("msgbroker Reconnect")

	time.AfterFunc(3*time.Second, func() {
		if err := b.Connect(); err != nil {
			if b.ReconnectionCount < b.ReconnectionLimit {
				b.ReconnectionCount++
				b.Reconnect()
				return
			}

			panic("msgBroker Cant connect in rabbitmq")
		}
		if err := b.BindAllRabbitQueues(); err != nil {
			logrus.WithFields(logrus.Fields{
				"error": fmt.Sprintf("%+v\n", err),
			}).Error("msgBroker Error on BindAllRabbitQueues")

			panic("msgBroker Error on BindAllRabbitQueues")
		}

		b.ReconnectionCount = 0
	})

	return nil
}

func (b *RabbitMQBroker) GetRabbitQueue(queueName string) (*RabbitQueue, error) {
	q := b.RabbitQueues[queueName]
	if q != nil {
		return q, nil
	}

	return b.CreateRabbitQueue(queueName)
}

// Create queue and connect with a new channel
func (b *RabbitMQBroker) CreateRabbitQueue(queueName string) (*RabbitQueue, error) {
	var err error

	q := &RabbitQueue{
		Name: queueName,
	}

	b.RabbitQueues[queueName] = q

	err = b.BindRabbitQueue(q)
	if err != nil {
		return q, err
	}

	return q, nil
}

func (b *RabbitMQBroker) BindAllRabbitQueues() error {
	for _, q := range b.RabbitQueues {
		err := b.BindRabbitQueue(q)
		if err != nil {
			return errors.Wrap(err, "error on bindAllRabbitQueues")
		}
	}

	return nil
}

func (b *RabbitMQBroker) BindRabbitQueue(q *RabbitQueue) error {
	var err error

	q.Channel, err = b.Connection.Channel()
	if err != nil {
		return errors.Wrap(err, "Error on create channel")
	}

	if err = q.Channel.ExchangeDeclare(
		q.Name,   // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // noWait
		nil,      // arguments
	); err != nil {
		return fmt.Errorf("Error in Exchange Declare: %s", err)
	}

	if _, err := q.Channel.QueueDeclare(
		q.Name,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	); err != nil {
		return errors.Wrap(err, "error in declaring the queue "+q.Name)
	}

	if err := q.Channel.QueueBind(
		q.Name, // queueName
		q.Name, // key
		q.Name, // exchange
		false,  // noWait
		nil,    // arguments
	); err != nil {
		return errors.Wrap(err, "RabbitQueue  Bind error queue: "+q.Name)
	}

	if q.IsConsuming {
		q.IsConsuming = false
		// b.Subscribe(q.Name, q.Handler)
	}

	return nil
}
