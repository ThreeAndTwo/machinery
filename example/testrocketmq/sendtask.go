package main

import (
	"context"
	"github.com/RichardKnop/machinery/example/testrocketmq/worker"
)

func main() {
	worker.SendHelloWorldTask(context.Background())
}
