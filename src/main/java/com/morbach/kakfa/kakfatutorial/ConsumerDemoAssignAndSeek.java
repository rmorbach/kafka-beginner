package com.morbach.kakfa.kakfatutorial;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignAndSeek {

public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		Properties properties = new Properties();
    	properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	// Define type of data read from Kafka
    	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    	// earliest, latest, none
    	properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	
    	// create a consumer
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    	
    	// assign and seek used to replay data or fetch a specific message
    	TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
    	consumer.assign(Arrays.asList(partitionToReadFrom));		
    	consumer.seek(partitionToReadFrom, 15L);
    	
    	int messagesCounter = 5;
    	
    	while(true) {
    		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
    		logger.info(records.toString());
    		for (ConsumerRecord<String, String> record: records) {
    			logger.info("Key: " + record.key() + " - Value: " + record.value());
    		}
    		if(messagesCounter == 0) {
    			break;
    		}
    		messagesCounter--;    		
    	}   
    	logger.info("Exiting application");
    	consumer.close();
	}
	
	
}
