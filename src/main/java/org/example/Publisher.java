package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.logging.Logger;

public class Publisher {

    private static final Logger logger = Logger.getLogger("Kafka-Publisher");

    private static final String topicName = "MySecond";

    public static void main(String[] args) {
        Properties kafkaProducerProperties = KafkaServerConfig.getProducerProperties();
        KafkaProducer<Integer, String> firstProducer = new KafkaProducer<>(kafkaProducerProperties);
        logger.info("firstProducer has been started!!");

//        int partition = 7;
//        // stuck with error in callback results
//        // SEVERE: record exception thrown: org.apache.kafka.common.errors.TimeoutException: Topic MySecond not present in metadata after 60000 ms.
        int partition = 1;
//        int fixedKey = 2;

        ProducerRecord<Integer, String> record;
        for (int i = 1; i <= 7; i++) {
//            record = new ProducerRecord<>("firstTopic", fixedKey, "msg" + i);
            record = new ProducerRecord<>(topicName, partition, i, "msg" + i);
            firstProducer.send(record, Publisher::sendingRecordCallback);
            firstProducer.flush();
        }

        firstProducer.close();
    }

    private static void sendingRecordCallback(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            logger.info("Record successfully sent, the details as: \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
        } else {
            logger.severe("record exception thrown: " + exception);
        }
    }

}
