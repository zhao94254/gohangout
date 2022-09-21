// 使用 https://github.com/Shopify/sarama kafka 库
package main

import (
	sysjson "encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/childe/healer"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/childe/gohangout/codec"
	"github.com/golang/glog"
)

type KafkaSaramaInput struct {
	config         map[interface{}]interface{}
	decorateEvents bool

	messages chan *sarama.ConsumerMessage

	decoder codec.Decoder

	groupConsumers []*cluster.Consumer
}

func New(config map[interface{}]interface{}) interface{} {
	var (
		codertype      = "plain"
		decorateEvents = false
		topics         map[interface{}]interface{}
		assign         map[string]map[int32]struct{}
	)
	glog.Infof("Input 配置：%+v", config)
	consumer_settings := make(map[string]interface{})
	if v, ok := config["consumer_settings"]; !ok {
		glog.Fatal("kafka input must have consumer_settings")
	} else {
		for x, y := range v.(map[interface{}]interface{}) {
			if reflect.TypeOf(y).Kind() == reflect.Map {
				yy := make(map[string]interface{})
				for kk, vv := range y.(map[interface{}]interface{}) {
					yy[kk.(string)] = vv
				}
				consumer_settings[x.(string)] = yy
			} else {
				consumer_settings[x.(string)] = y
			}
		}
	}
	if v, ok := config["topic"]; ok {
		topics = v.(map[interface{}]interface{})
	} else {
		topics = nil
	}
	if v, ok := config["assign"]; ok {
		assign = make(map[string]map[int32]struct{})
		for topicName, partitions := range v.(map[interface{}]interface{}) {
			assign[topicName.(string)] = make(map[int32]struct{})
			for _, p := range partitions.([]interface{}) {
				assign[topicName.(string)][int32(p.(int))] = struct{}{}
			}
		}
	} else {
		assign = nil
	}

	if topics == nil && assign == nil {
		glog.Fatal("either topic or assign should be set")
	}
	if topics != nil && assign != nil {
		glog.Fatal("topic and assign can not be both set")
	}

	if codecV, ok := config["codec"]; ok {
		codertype = codecV.(string)
	}

	if decorateEventsV, ok := config["decorate_events"]; ok {
		decorateEvents = decorateEventsV.(bool)
	}

	kafkaSaramaInput := &KafkaSaramaInput{
		config:         config,
		decorateEvents: decorateEvents,
		messages:       make(chan *sarama.ConsumerMessage, 10),

		decoder: codec.NewDecoder(codertype),
	}

	brokers, groupID, consumerConfig, err := getConsumerConfig(consumer_settings)
	if err != nil {
		glog.Fatalf("error in consumer settings: %s", err)
	}

	// 开启分区模式
	if assign != nil {
		consumerConfig.Group.Mode = cluster.ConsumerModePartitions
	}

	// GroupConsumer
	if topics != nil {
		for topic, threadCount := range topics {
			glog.Infoln(brokers, groupID, topic)
			glog.Infof("kafka 消费者配置：%+v", consumerConfig)
			for i := 0; i < threadCount.(int); i++ {
				c, err := cluster.NewConsumer(brokers, groupID, []string{topic.(string)}, consumerConfig)
				if err != nil {
					glog.Fatalf("could not init GroupConsumer: %s", err)
				}
				kafkaSaramaInput.groupConsumers = append(kafkaSaramaInput.groupConsumers, c)

				// 接收数据
				go func() {
					//glog.Infoln("start c.Messages() 消费")
					for msg := range c.Messages() {
						//glog.Infoln("c.Messages() 消费")
						// mark message as processed
						c.MarkOffset(msg, "")
						kafkaSaramaInput.messages <- msg
					}
				}()

				// 打印错误
				go func() {
					//glog.Infoln("start c.Errors()")
					for err := range c.Errors() {
						glog.Errorf("kafka consumer error:  %+v\n", err)
					}
				}()
			}
		}
	} else {
		for topic, partitions := range assign {
			glog.Infoln(brokers, groupID, topic)
			glog.Infof("kafka 消费者配置：%+v", consumerConfig)
			// 一个分区一个消费者
			c, err := cluster.NewConsumer(brokers, groupID, []string{topic}, consumerConfig)
			if err != nil {
				glog.Fatalf("could not init GroupConsumer: %s\n", err)
			}

			kafkaSaramaInput.groupConsumers = append(kafkaSaramaInput.groupConsumers, c)

			go func() {
				for {
					select {
					// 拿出全部分区判断是否为指定的分区
					case part, ok := <-c.Partitions():
						if !ok {
							glog.Errorln("kafka consumer error: Partitions() channel closed")
							return
						}
						// 开始消费
						go func(pc cluster.PartitionConsumer) {
							// 不是指定分区，则取消消费
							for msg := range pc.Messages() {
								if _, ok := partitions[msg.Partition]; !ok {
									return
								}
								c.MarkOffset(msg, "")
								kafkaSaramaInput.messages <- msg
							}
						}(part)
					}
				}
			}()

			go func() {
				//glog.Infoln("start c.Errors()")
				for err := range c.Errors() {
					glog.Errorf("kafka consumer error:  %+v\n", err)
				}
			}()
		}
	}

	return kafkaSaramaInput
}

func (p *KafkaSaramaInput) ReadOneEvent() map[string]interface{} {
	//glog.Infoln("ReadOneEvent 消费")
	message, ok := <-p.messages
	if ok {
		event := p.decoder.Decode(message.Value)
		if p.decorateEvents {
			kafkaMeta := make(map[string]interface{})
			kafkaMeta["topic"] = message.Topic
			kafkaMeta["partition"] = message.Partition
			kafkaMeta["offset"] = message.Offset
			event["@metadata"] = map[string]interface{}{"kafka": kafkaMeta}
			event["test"] = "test"
		}
		return event
	}
	return nil
}

func (p *KafkaSaramaInput) Shutdown() {
	for _, v := range p.groupConsumers {
		err := v.Close()
		if err != nil {
			glog.Error("kafka consumer close error: ", err.Error())
		}
	}
}

// 未适配
// TLS 全部
// offsets.storage
// connect.timeout.ms
// timeout.ms.for.eachapi
func getConsumerConfig(config map[string]interface{}) (brokers []string, groupId string, cfg *cluster.Config, err error) {
	b, err := sysjson.Marshal(config)
	if err != nil {
		return
	}
	dc := defaultConsumerConfig()
	err = sysjson.Unmarshal(b, dc)
	if err != nil {
		return
	}

	brokers = strings.Split(dc.BootstrapServers, ",")
	groupId = dc.GroupID

	cfg = cluster.NewConfig()
	cfg.Version, err = sarama.ParseKafkaVersion(dc.Version)
	if err != nil {
		return
	}

	cfg.ClientID = dc.ClientID
	//cfg.Metadata.Timeout = time.Duration(dc.MetadataMaxAgeMS) * time.Millisecond

	cfg.Consumer.Retry.Backoff = time.Duration(dc.RetryBackOffMS) * time.Millisecond
	cfg.Consumer.Group.Session.Timeout = time.Duration(dc.SessionTimeoutMS) * time.Millisecond
	cfg.Consumer.MaxWaitTime = time.Duration(dc.FetchMaxWaitMS) * time.Millisecond
	cfg.Consumer.Fetch.Max = dc.FetchMaxBytes
	cfg.Consumer.Fetch.Min = dc.FetchMinBytes

	if dc.FromBeginning {
		glog.Infoln("OffsetOldest")
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		glog.Infoln("OffsetNewest")
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	//cfg.Consumer.Offsets.AutoCommit.Enable = dc.AutoCommit
	cfg.Consumer.Offsets.CommitInterval = time.Duration(dc.AutoCommitIntervalMS) * time.Millisecond

	cfg.Net.DialTimeout = time.Duration(dc.NetConfig.TimeoutMS) * time.Millisecond
	cfg.Net.KeepAlive = time.Duration(dc.NetConfig.KeepAliveMS) * time.Millisecond

	if dc.SaslConfig != nil {
		cfg.Net.SASL.User = dc.SaslConfig.SaslUser
		cfg.Net.SASL.Password = dc.SaslConfig.SaslPassword
		cfg.Net.SASL.Mechanism = sarama.SASLMechanism(dc.SaslConfig.SaslMechanism)
	}

	//cfg.Net.TLS.Enable = dc.TLSEnabled

	return
}

type ConsumerConfig struct {
	healer.ConsumerConfig
	Version string `json:"version"`
}

func defaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		ConsumerConfig: *healer.DefaultConsumerConfig(),
		Version:        "0.9.0.0",
	}
}