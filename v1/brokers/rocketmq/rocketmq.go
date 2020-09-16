/**
 * Created by Goland.
 * Description:
 * User: 礼凯
 * Date: 2020/8/30 5:10 下午
 */
package rocketmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	rocketmq "github.com/apache/rocketmq-client-go/core"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
)

type ROCKETMQConnection struct {
	queueName    string
	connection   *amqp.Connection
	channel      *amqp.Channel
	queue        amqp.Queue
	confirmation <-chan amqp.Confirmation
	errorchan    <-chan *amqp.Error
	cleanup      chan struct{}
}

type Broker struct {
	common.Broker
	common.ROCKETMQConnector

	host      string
	group     string
	topic     string
	accessKey string
	secretKey string
	channel   string

	processingWG sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal

	connections      map[string]*ROCKETMQConnection
	connectionsMutex sync.RWMutex
}

func New(cnf *config.Config, host, group, topic, accessKey, secretKey, channel string) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.host = host
	b.group = group
	b.topic = topic
	b.accessKey = accessKey
	b.secretKey = secretKey
	b.channel = channel

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	rocketmqConfig := b.ROCKETMQConnector.RocketmqConsumerConfig(b.host, b.group, b.accessKey, b.secretKey, b.channel)
	consumer, err := rocketmq.NewPushConsumer(rocketmqConfig)
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		log.ERROR.Print("[*] create Consumer failed, error:")

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	ch := make(chan interface{})
	var count = (int64)(1000000)

	consumer.Subscribe(b.topic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		log.INFO.Print("[*]A message received, MessageID:%s, Body:%s \n", msg.MessageID, msg.Body)



		if atomic.AddInt64(&count, -1) <= 0 {
			ch <- "quit"
		}
		//消费成功回复 ConsumeSuccess，消费失败回复 ReConsumeLater。此时会触发消费重试。
		return rocketmq.ConsumeSuccess
	})
	err = consumer.Start()
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		log.ERROR.Print("[*]consumer start failed", err)

		if b.GetRetry() {
			return b.GetRetry(), err
		}

		return b.GetRetry(), errs.ErrConsumerStopped
	}
	log.INFO.Print("consumer: %s started...\n", consumer)
	<-ch
	//请保持消费者一直处于运行状态。
	err = consumer.Shutdown()
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		log.ERROR.Print("[*]consumer shutdown failed", err)

		if b.GetRetry() {
			return b.GetRetry(), err
		}

		return b.GetRetry(), errs.ErrConsumerStopped
	}
	log.INFO.Print("consumer has shutdown", err)

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()

	// todo 待测试
	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}


func (b *Broker) CloseConnections() error {
	b.connectionsMutex.Lock()
	defer b.connectionsMutex.Unlock()

	// todo  rocketmq 如何断开连接?

	return nil
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {

	rocketmqConfig := b.ROCKETMQConnector.RocketmqProducerConfig(b.host, b.group, b.accessKey, b.secretKey, b.channel)
	producer, err := rocketmq.NewProducer(rocketmqConfig)
	if err != nil {
		log.ERROR.Print("create common producer failed, error:", err)
		return err
	}
	//请确保参数设置完成之后启动 Producer。
	err = producer.Start()
	if err != nil {
		log.ERROR.Print("start common producer error", err)
		return err
	}
	defer producer.Shutdown()
	log.INFO.Print("Common producer: %s started... \n", producer)
	//for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("%s-%d", "Hello,Common MQ Message")
		//发送消息时请设置您在阿里云 RocketMQ 控制台上申请的 Topic。
		result, err := producer.SendMessageSync(&rocketmq.Message{Topic: b.topic, Body: msg})
		if err != nil {
			fmt.Println("Error:", err)
		}
		fmt.Printf("send message: %s result: %s\n", msg, result)
	//}
	fmt.Println("shutdown common producer.")


	return nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan amqp.Delivery, concurrency int, taskProcessor iface.TaskProcessor, amqpCloseChan <-chan *amqp.Error) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	// make channel with a capacity makes it become a buffered channel so that a worker which wants to
	// push an error to `errorsChan` doesn't need to be blocked while the for-loop is blocked waiting
	// a worker, that is, it avoids a possible deadlock
	errorsChan := make(chan error, 1)

	for {
		select {
		case amqpErr := <-amqpCloseChan:
			return amqpErr
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor, true); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.GetStopChan():
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery amqp.Delivery, taskProcessor iface.TaskProcessor, ack bool) error {
	if len(delivery.Body) == 0 {
		delivery.Nack(true, false)                     // multiple, requeue
		return errors.New("Received an empty message") // RabbitMQ down?
	}

	var multiple, requeue = false, false

	// Unmarshal message body into signature struct
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		delivery.Nack(multiple, requeue)
		return errs.NewErrCouldNotUnmarshalTaskSignature(delivery.Body, err)
	}

	// If the task is not registered, we nack it and requeue,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		requeue = true
		log.INFO.Printf("Task not registered with this worker. Requeing message: %s", delivery.Body)

		if !signature.IgnoreWhenTaskNotRegistered {
			delivery.Nack(multiple, requeue)
		}

		return nil
	}

	log.DEBUG.Printf("Received new message: %s", delivery.Body)

	err := taskProcessor.Process(signature)
	if ack {
		delivery.Ack(multiple)
	}
	return err
}

// delay a task by delayDuration miliseconds, the way it works is a new queue
// is created without any consumers, the message is then published to this queue
// with appropriate ttl expiration headers, after the expiration, it is sent to
// the proper queue with consumers
func (b *Broker) delay(signature *tasks.Signature, delayMs int64) error {

	return nil
}

func (b *Broker) isDirectExchange() bool {
	return b.GetConfig().AMQP != nil && b.GetConfig().AMQP.ExchangeType == "direct"
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	if b.isDirectExchange() {
		// The routing algorithm behind a direct exchange is simple - a message goes
		// to the queues whose binding key exactly matches the routing key of the message.
		s.RoutingKey = b.GetConfig().AMQP.BindingKey
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}

// Helper type for GetPendingTasks to accumulate signatures
type sigDumper struct {
	customQueue string
	Signatures  []*tasks.Signature
}

func (s *sigDumper) Process(sig *tasks.Signature) error {
	s.Signatures = append(s.Signatures, sig)
	return nil
}

func (s *sigDumper) CustomQueue() string {
	return s.customQueue
}

func (_ *sigDumper) PreConsumeHandler() bool {
	return true
}

func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {

	dumper := &sigDumper{customQueue: queue}

	return dumper.Signatures, nil
}
