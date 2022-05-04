# Catupiri message broker RabbitMQ client

RabbitMQ client to send and receive messages with RabbitMQ services

## Usage

See https://github.com/go-catupiry/msgbroker

```go
uri := app.GetConfiguration().GetF("MSG_BROOKER_URI", "amqp://rabbitmq:rabbitmq@localhost:5672/")
p.Client = NewRabbitMQBroker(uri)
```