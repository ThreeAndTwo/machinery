/**
 * Created by Goland.
 * Description:
 * User: 礼凯
 * Date: 2020/8/30 5:36 下午
 */
package common

import rocketmq "github.com/apache/rocketmq-client-go/core"

type ROCKETMQConnector struct{}

func (rc *ROCKETMQConnector) RocketmqConsumerConfig(url, group, accessKey, secretKey, channel string) *rocketmq.PushConsumerConfig {
	pConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID: group,
			NameServer: url,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: accessKey,
				SecretKey: secretKey,
				Channel: channel,
			},
		},
		//设置使用集群模式。
		Model: rocketmq.Clustering,
		//设置该消费者为普通消息消费。
		ConsumerModel: rocketmq.CoCurrently,
	}
	return pConfig
}

func (rc *ROCKETMQConnector) RocketmqProducerConfig(url, group, accessKey, secretKey, channel string) *rocketmq.ProducerConfig {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID: group,
			NameServer: url,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: accessKey,
				SecretKey: secretKey,
				Channel: channel,
			},
		},

		// todo，不能写死
		//主动设置该实例用于发送普通消息。
		ProducerModel: rocketmq.CommonProducer,
	}
	return pConfig
}

