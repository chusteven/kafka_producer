# Overview

# Running locally

To test ensure that you have a local version of Kafka installed. I followed [these directions](https://kafka.apache.org/quickstart), which eventually require you to start a few daemons:

```
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties
$ bin/kafka-console-consumer.sh --topic <YOUR TOPIC> --from-beginning --bootstrap-server localhost:9092
{"data":{"id":"1482496347722899457","text":"@editseer @freshyuchiha Done"},"matching_rules":[{"id":"1482495691595288578","tag":"Sampled tweets about feelings, in English"}]}
{"data":{"id":"1482496349828378625","text":"@PittCabe Always so wild to me how America and Americans just shrug even the most violent and disgusting acts off because it doesn't feel good to remember unless it benefits from it."},"matching_rules":[{"id":"1482495691595288578","tag":"Sampled tweets about feelings, in English"}]}
{"data":{"id":"1482496349832720393","text":"RT @Arlemish: Day 7 of #100DaysOfCode:\nMy guys, I'm done with the Functional Programming section on the @freeCodeCamp course, I am so happy…"},"matching_rules":[{"id":"1482495691595288578","tag":"Sampled tweets about feelings, in English"}]}
...
```

Note: In order to consumer from a topic, it must be created first:

```
$ bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic <YOUR TOPIC> --bootstrap-server localhost:9092 --config 'retention.ms=172800000'
```

# Development
