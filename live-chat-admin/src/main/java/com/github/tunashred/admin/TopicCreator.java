package com.github.tunashred.admin;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Log4j2
public class TopicCreator {

    public static String createPackTopic(String topicName) throws IOException {
        String topic = "pack-" + topicName;
        if (!isValidKafkaTopicName(topic)) {
            log.error("Provided topic name does not follow rules of pack topics");
            return null;
        }

        return createTopic(topic, loadProperties("pack_topic.properties"));
    }

    public static String createTopic(String topicName) throws IOException {
        return createTopic(topicName, new Properties());
    }

    public static String createTopic(String topicName, Properties topicProps) throws IOException {
        try {
            Properties adminProps = loadProperties("admin.properties");
            Admin admin = Admin.create(adminProps);
            NewTopic newTopic = new NewTopic(topicName, Optional.empty(), Optional.empty());

            if (!topicProps.isEmpty()) {
                Map<String, String> configs = new HashMap<>();
                topicProps.stringPropertyNames().forEach(propName -> configs.put(propName, topicProps.getProperty(propName)));
                newTopic.configs(configs);
            }

            CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
            KafkaFuture<Void> future = result.values().get(topicName);

            future.get();
            waitUntilTopicExists((AdminClient) admin, topicName);
            log.info("Topic " + topicName + " created successfully!");

            return topicName;
        } catch (IOException e) {
            log.error("Unable to load admin properties from file.");
        } catch (ExecutionException e) {
            log.error("Exception occured while trying to create topic: ", e);
        } catch (InterruptedException e) {
            log.warn("Operation interrupted: ", e);
            Thread.currentThread().interrupt();
        }
        return null;
    }

    private static void waitUntilTopicExists(AdminClient admin, String topicName) throws ExecutionException, InterruptedException {
        final int timeoutMs = 10_000;
        long start = System.currentTimeMillis();
        long end = start + timeoutMs;

        while (System.currentTimeMillis() < end) {
            ListTopicsResult topics = admin.listTopics();
            Set<String> names = topics.names().get();
            if (names.contains(topicName)) {
                return;
            }
            Thread.sleep(1000);
        }
        log.error("Timed out while waiting for topic '" + topicName + "' to appear for " + timeoutMs + " ms");
    }

    public static void deleteTopic(String topicName) {
        try {
            Properties adminProps = loadProperties("admin.properties");
            Admin admin = Admin.create(adminProps);

            DeleteTopicsResult result = admin.deleteTopics(Collections.singleton(topicName));
            KafkaFuture<Void> future = result.all();
            future.get();
            waitUntilTopicDeleted((AdminClient) admin, topicName);

            log.info("Topic " + topicName + " deleted successfully");
        } catch (IOException e) {
            log.error("Unable to load admin properties from file.");
        } catch (ExecutionException e) {
            log.error("Exception occured while trying to delete topic: ", e);
        } catch (InterruptedException e) {
            log.warn("Operation interrupted: ", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Exception occured while waiting for the topic to be deleted");
        }
    }

    private static void waitUntilTopicDeleted(AdminClient admin, String topicName) throws Exception {
        final int timeoutMs = 10_000;
        long start = System.currentTimeMillis();
        long end = start + timeoutMs;
        while (System.currentTimeMillis() < end) {
            try {
                Set<String> topics = admin.listTopics().names().get();
                if (!topics.contains(topicName)) {
                    return;
                }
            } catch (Exception e) {
                log.error("Exception occured while waiting for topic '" + topicName + "' to be deleted");
            }
            Thread.sleep(1000);
        }
        log.error("Timed out while waiting for topic '" + topicName + "' to be deleted, for " + timeoutMs + " ms");
    }

    private static Properties loadProperties(String filePath) throws IOException {
        return loadProperties(filePath, new Properties());
    }

    private static Properties loadProperties(String filePath, Properties properties) throws IOException {
        Properties props = new Properties();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream input = classLoader.getResourceAsStream(filePath)) {
            props.load(input);
            props.putAll(properties);
        }
        return props;
    }

    private static boolean isValidKafkaTopicName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        if (name.equals(".") || name.equals("..")) {
            return false;
        }
        if (name.length() > 249) {
            return false;
        }
        return name.matches("[a-zA-Z0-9._-]+");
    }
}
