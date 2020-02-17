package com.morbach.streams.filter.tweets;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {

	private static Integer MIN_FOLLOWERS = 10000;
	private static String TO_TOPIC = "important_tweets";
	private static String TOPIC = "twitter_tweets";
	
	public static void main(String[] args) {
		// create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		// create topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String, String> inputTopic = streamsBuilder.stream(TOPIC);

		KStream<String, String> filteredStream = inputTopic.filter(
				(key, jsonTweet) -> extractUserFollowersFromTweet(jsonTweet) > MIN_FOLLOWERS
				);
		filteredStream.to(TO_TOPIC);
		
		// build the topology
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		
		// start the application
		kafkaStreams.start();
		
	}
	
	private static Integer extractUserFollowersFromTweet(String jsonString) {  
		try {
		return JsonParser.parseString(jsonString)
				.getAsJsonObject()
				.get("user")
				.getAsJsonObject()
				.get("followers_count").getAsInt();
		} catch(Exception e) {
			return 0;
		}
	}
	
}
