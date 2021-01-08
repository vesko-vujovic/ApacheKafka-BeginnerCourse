package com.github.simpleexample.kafka.tutorial1;

import jdk.nashorn.internal.codegen.CompilerConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // send data
        for (int i = 0; i < 10; i++) {

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world " + i);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every  time  a record is successfully sent or and exception is thrown

                    if (exception == null) {
                        logger.info("Receive metadata. \n" +
                                "Topic: " + metadata.topic()  + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error("Erorr while producing");
                    }
                }
            });
        }


        // flush data
        producer.flush();
        // close the data
        producer.close();
    }
}
