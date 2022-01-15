# Overview

# Running locally

To test ensure that you have a local version of Kafka installed. I followed [these directions](https://kafka.apache.org/quickstart), which eventually require you to start a few daemons:

```
pi@raspberrypi:~/pg/kafka_2.13-3.0.0 $ bin/zookeeper-server-start.sh config/zookeeper.properties
pi@raspberrypi:~/pg/kafka_2.13-3.0.0 $ bin/kafka-server-start.sh config/server.properties
pi@raspberrypi:~/pg/kafka_2.13-3.0.0 $ bin/kafka-console-consumer.sh --topic <YOUR TOPIC> --from-beginning --bootstrap-server localhost:9092
```

Note: In order to consumer from a topic, it must be created first:

```
pi@raspberrypi:~/pg/kafka_2.13-3.0.0 $ bin/kafka-console-consumer.sh --topic <YOUR TOPIC> --from-beginning --bootstrap-server localhost:9092
```

# Development
