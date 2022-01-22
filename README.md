# Overview

# Running locally

To test ensure that you have a local version of Kafka installed. I followed [these directions](https://kafka.apache.org/quickstart). Once you have Kafka on your system, you'll need to (a) start Zookeeper, (b) start Kafka, and (c) create a new topic.

```
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties
$ bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 \
  --topic <YOUR TOPIC> --bootstrap-server localhost:9092 \
  --config 'retention.ms=172800000'
```

# Development
