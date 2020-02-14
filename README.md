# kafka-beginner
Udemy's course code

## Start Kafka

Start Zookeeper first

```bash
$ zookeeper-server-start.sh zookeeper.properties
```

```bash
$ kafka-server-start.sh server.properties
```

## Create a topic

```bash
$ kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic twitter_tweets --create --partitions 3 --replication-factor 1
```

## Publish to a topic

```bash
$ kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic twitter_tweets
```

# Subscribe to a topic

Read from the beginning.

```bash
$ kafka-console-consumer.sh --bootstrap-seer 127.0.0.1:9092 --topic twitter_tweets --from-beginning
```


