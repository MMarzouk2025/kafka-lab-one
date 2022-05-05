package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.logging.Logger;

public class Publisher {

    private static final Logger logger = Logger.getLogger("Kafka-Publisher");

    public static void main(String[] args) {
        Properties kafkaServerProperties = KafkaServerConfig.getDefaultProperties();
        KafkaProducer<Integer, String> firstProducer = new KafkaProducer<>(kafkaServerProperties);
        logger.info("firstProducer has been started!!");

        ProducerRecord<Integer, String> record;
        for (int i = 1; i <= 7; i++) {
            record = new ProducerRecord<>("firstTopic", i, "msg" + i);
            firstProducer.send(record, Publisher::sendingRecordCallback);
            firstProducer.flush();
        }

        firstProducer.close();
    }

    private static void sendingRecordCallback(RecordMetadata recordMetadata, Exception exception) {
        System.out.println("record sent details: " + recordMetadata);
        if (exception != null) {
            System.out.println("record exception thrown: " + exception);
        }
    }

}
