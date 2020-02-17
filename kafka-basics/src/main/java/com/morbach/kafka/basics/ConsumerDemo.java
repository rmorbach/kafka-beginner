package com.morbach.kafka.basics;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		Properties properties = new Properties();
    	properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	// Define type of data read from Kakfa
    	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    	// earliest, latest, none
    	properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	
    	// create a consumer
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    	
    	// subscribe to a topic
    	consumer.subscribe(Collections.singleton("first_topic"));
		
    	while(true) {
    		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
    		logger.info(records.toString());
    		for (ConsumerRecord<String, String> record: records) {
    			logger.info("Key: " + record.key() + " - Value: " + record.value());
    		}
    		
    	}   
	}
	
}
