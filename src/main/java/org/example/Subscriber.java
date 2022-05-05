package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

public class Subscriber {

    private static final Logger logger = Logger.getLogger(Subscriber.class.getName());

    private static final String topicName = "MySecond";
    private static final int MAX_MESSAGES_COUNT = 70;

    public static void main(String[] args) {
        Properties kafkaConsumerProperties = KafkaServerConfig.getConsumerProperties();
        KafkaConsumer<Integer, String> firstConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
        logger.info("firstConsumer has been started!!");

        firstConsumer.subscribe(List.of(topicName));
        int numOfMsgsReceived = 0;
        do {
            ConsumerRecords<Integer, String> records = firstConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<Integer, String> record : records) {
                logger.info("New message has been received!! => key: " + record.key() + ", value: " + record.value()
                        + ", at: " + LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), TimeZone.getDefault().toZoneId())
                        + "\nPartition: " + record.partition() + " - Offset: " + record.offset());
                numOfMsgsReceived++;
            }

//            firstConsumer.commitAsync((offsets, exception) -> {
//                if (exception != null) {
//                    throw new RuntimeException("error while handling offset committing for new received message", exception);
//                }
//                logger.info("message has been committed successfully!!");
//                logger.info(offsets.toString());
//            });

        } while (numOfMsgsReceived != MAX_MESSAGES_COUNT);

        firstConsumer.close();
    }

}
