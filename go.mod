module github.com/ThreeAndTwo/machinery

go 1.14

replace github.com/RichardKnop/machinery => github.com/ThreeAndTwo/machinery v1.9.2 // indirect

require (
	cloud.google.com/go/pubsub v1.5.0
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/RichardKnop/machinery v0.0.0-00010101000000-000000000000
	github.com/RichardKnop/redsync v1.2.0
	github.com/apache/rocketmq-client-go v1.2.4
	github.com/aws/aws-sdk-go v1.33.6
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/go-redis/redis/v8 v8.0.0-beta.6
	github.com/gomodule/redigo v1.8.2
	github.com/google/uuid v1.1.1
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.6.1
	github.com/urfave/cli v1.22.4
	go.mongodb.org/mongo-driver v1.3.5
	gopkg.in/yaml.v2 v2.3.0
)

//git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
