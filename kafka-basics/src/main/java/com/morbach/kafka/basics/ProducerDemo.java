package com.morbach.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hello world!
 *
 */
public class ProducerDemo 
{
    public static void main( String[] args ){
        // create producer properties
    	Properties properties = new Properties();
    	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    	// Define type of data sent to Kakfa
    	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	// create producer
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    	// send data
    	ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "value");
    	// Sends asynchronosly
    	producer.send(producerRecord);
    	// flush data
    	producer.flush();
    	// flush data
    	producer.close();
    	
    }
}
