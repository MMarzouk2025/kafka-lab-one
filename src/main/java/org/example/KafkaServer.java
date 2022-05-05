package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class KafkaServer {

    private static final Logger logger = Logger.getLogger("Kafka-Server");

    public static void main(String[] args) {
        createTopic("MySecond", 5);
//        createTopic("MyThird", 2);
    }

    private static void createTopic(String topicName, int partitionsNo) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerConfig.BROKERS);

        boolean topicAlreadyExisting;
        try (AdminClient admin = AdminClient.create(config)) {
            topicAlreadyExisting = admin.listTopics().names().get()
                    .stream()
                    .anyMatch(existingTopicName -> existingTopicName.equals(topicName));

            if (topicAlreadyExisting) {
                logger.warning("topic already exists: " + topicName);
            } else {
                logger.info("creating topic: " + topicName);
                NewTopic newTopic = new NewTopic(topicName, partitionsNo, (short) 1);
                admin.createTopics(Collections.singleton(newTopic)).all()
                        .whenComplete((unused, throwable) -> {
                            if (throwable == null) {
                                logger.info("topic " + topicName + " has been created successfully!!");
                            } else {
                                logger.severe("error while creating topic " + topicName
                                        + "\nException: " + throwable);
                            }
                        });
            }

            admin.describeTopics(Collections.singleton(topicName)).allTopicNames()
                    .whenComplete((stringTopicDescriptionMap, throwable) -> {
                        if (throwable != null) {
                            throwable.printStackTrace();
                        }
                        stringTopicDescriptionMap.forEach((topic, topicDescription) -> {
                            logger.info("topic " + topicName + " description =>>");
                            logger.info("partitions: " + topicDescription.partitions().size());
                            logger.info("partitions ids: " + topicDescription.partitions()
                                    .stream()
                                    .map(p -> Integer.toString(p.partition()))
                                    .collect(Collectors.joining(",")));
                        });
                    });
        } catch (ExecutionException e) {
            logger.severe("ExecutionException: " + e);
        } catch (InterruptedException e) {
            logger.severe("InterruptedException: " + e);
        }

    }

}
