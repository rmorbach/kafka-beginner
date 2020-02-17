package com.morbach.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	final static String TWITTER_API_KEY = "";
	final static String TWITTER_API_TOKEN = "";
	final static String TWITTER_CONSUMER_KEY = "";
	final static String TWITTER_CONSUMER_TOKEN = "";

	final static List<String> TWITTER_TERMS = Lists.newArrayList("politics", "soccer", "bitcoin");
	
	public static void main(String[] args) {
		TwitterProducer twitter = new TwitterProducer();
		twitter.run();
	}

	public void run() {

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

		// create twitter client
		Client client = createTwitterClient(msgQueue);
		client.connect();
		// create kafka producer

		KafkaProducer<String, String> producer = createKafkaProducer();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutting down the application");
			client.stop();
			producer.close();
		}));

		// send tweets to kafka
		while (!client.isDone()) {
			try {
				String message = msgQueue.poll(5, TimeUnit.SECONDS);
				if (message != null) {
					logger.info("Received message: " + message);
					ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("twitter_tweets",
							message);
					producer.send(producerRecord, new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							if (exception != null) {
								logger.error("Failed saving tweet " + exception.getLocalizedMessage());
							}
						}
					});
				}
			} catch (Exception e) {
				client.stop();
				producer.close();
			}
		}
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		// Define type of data sent to Kafka
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// create safer producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); 
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5)); 

		// High throughput 
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32");		
		
		return new KafkaProducer<String, String>(properties);
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = TWITTER_TERMS;
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_TOKEN, TWITTER_API_KEY,
				TWITTER_API_TOKEN);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();

	}
}
