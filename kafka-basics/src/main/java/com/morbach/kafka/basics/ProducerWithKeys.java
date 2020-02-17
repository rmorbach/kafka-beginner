package com.morbach.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {

	public static void main(String[] args) throws Exception {

		final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getName());

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		// Define type of data sent to Kakfa
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {

			String topic = "first_topic";
			String value = "Hello " + i;

			String key = "id_" + i;
			
			logger.info("Key " + key);


			// send data
			ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, key, value);
			// Sends asynchronosly
			producer.send(producerRecord, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// Record has been sent
					if (exception != null) {
						logger.info("Received new metadata \n" + "Topic " + metadata.topic() + "\n" + "Partition "
								+ metadata.partition() + "\n" + "Offset " + metadata.offset() + "\n" + "Timestamp "
								+ metadata.timestamp() + "\n");
					} else {
						// Something wrong happening
						logger.error("Error while producing", exception);
					}

				}
			}).get(); // Make send synchronous (BAD PRACTICE)
		}
		// flush data
		producer.flush();
		// flush data
		producer.close();
	}

}
