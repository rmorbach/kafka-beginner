package com.morbach.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	public static void main(String[] args) {

		RestHighLevelClient client = createElasticSearchClient();
		try {

			KafkaConsumer<String, String> consumer = createConsumer();

			// subscribe to a topic
			consumer.subscribe(Collections.singleton("twitter_tweets"));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
				System.out.println(records.count());
				
				BulkRequest bulkRequest = new BulkRequest();
				for (ConsumerRecord<String, String> record : records) {
					// Kafka generic id
					// String identifier = record.topic() + "_" + record.partition() + "_" +
					// record.offset();

					// Use twitter id
					String identifier = extractIdFromTweet(record.value());

					IndexRequest indexRequest = new IndexRequest("twitter").type("tweets")
							.source(record.value(), XContentType.JSON).id(identifier);
					
					bulkRequest.add(indexRequest); // add to bulk request					
				}
				if (records.count() <= 0) { 
					continue;
				}
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				System.out.println("Commiting offsets...."); 
				
				consumer.commitSync();
				
				System.out.println("Offsets commited!");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String extractIdFromTweet(String jsonString) {
		return JsonParser.parseString(jsonString).getAsJsonObject().get("id").getAsString();
	}

	private static KafkaConsumer<String, String> createConsumer() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		// Define type of data read from Kakfa
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
		// earliest, latest, none
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// disable offset auto commit
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

		// create a consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		return consumer;
	}

	private static RestHighLevelClient createElasticSearchClient() {

		String hostname = "";
		String username = "";
		String password = "";

		// Not necessary for local instance
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;

	}

}
