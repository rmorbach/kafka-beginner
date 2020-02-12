package com.morbach.kakfa.kakfatutorial;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}

	public ConsumerDemoWithThread() {

	}

	public void run() {
		CountDownLatch latch = new CountDownLatch(1);
		Runnable myConsumerThread = new ConsumerThread(latch, "first_topic");

		// Start the thread
		Thread myThread = new Thread(myConsumerThread);
		myThread.start();

		// shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			// Ugly cast
			((ConsumerThread) myConsumerThread).shutDown();
			try {
				latch.await();
			} catch (Exception e) { // ignore

			}
		}));
		try {
			latch.await();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	class ConsumerThread implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

		public ConsumerThread(CountDownLatch latch, String topic) {
			this.latch = latch;
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			// Define type of data read from Kakfa
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
			// earliest, latest, none
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			this.consumer = new KafkaConsumer<String, String>(properties);
			// subscribe to a topic
			consumer.subscribe(Collections.singleton(topic));
		}

		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
					logger.info(records.toString());
					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key: " + record.key() + " - Value: " + record.value());
					}

				}
			} catch (WakeupException e) {
				logger.info("Received shutdown");
			} finally {
				consumer.close();
				latch.countDown();
			}
		}

		public void shutDown() {
			consumer.wakeup(); // Interrupt consumer.poll. It will throw an exception
		}
	}

}
