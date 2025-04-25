package com.github.tunashred.admin;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Log4j2
public class TopicCreator {
    public static String createPackTopic(String topicName) throws IOException {
        String topic = packanizeTopicName(topicName);
        if (isInvalidKafkaTopicName(topic)) {
            log.error("Provided topic name does not follow the rules of pack topics");
            return null;
        }
        return createTopic(topic, loadProperties("pack_topic.properties"));
    }

    public static String createTopic(String topicName) {
        return createTopic(topicName, new Properties());
    }

    public static String createTopic(String topicName, Properties topicProps) {
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
            boolean success = waitUntilTopicExists((AdminClient) admin, topicName);
            if (success) {
                log.info("Topic {} created successfully!", topicName);
                return topicName;
            }
        } catch (IOException e) {
            log.error("Unable to load admin properties from file: ", e);
        } catch (ExecutionException e) {
            log.error("Exception occured while trying to create topic: ", e);
        } catch (InterruptedException e) {
            log.warn("Operation interrupted: ", e);
            Thread.currentThread().interrupt();
        }
        log.error("Errored while trying to create topic '{}'", topicName);
        return null;
    }

    public static boolean deletePackTopic(String topicName) {
        String topic = packanizeTopicName(topicName);
        if (isInvalidKafkaTopicName(topic)) {
            log.error("Provided topic name does not follow the rules of pack topics");
            return false;
        }
        return deleteTopic(topic);
    }

    public static boolean deleteTopic(String topicName) {
        boolean success = false;
        try {
            Properties adminProps = loadProperties("admin.properties");
            Admin admin = Admin.create(adminProps);

            if (!topicExists((AdminClient) admin, topicName)) {
                log.error("Topic '{}' does not exist", topicName);
                return false;
            }

            DeleteTopicsResult result = admin.deleteTopics(Collections.singleton(topicName));
            KafkaFuture<Void> future = result.all();
            future.get();
            success = waitUntilTopicDeleted((AdminClient) admin, topicName);

            if (success) {
                log.info("Topic {} deleted successfully", topicName);
            } else {
                log.error("Errored while trying to delete the topic '{}'", topicName);
            }
        } catch (IOException e) {
            log.error("Unable to load admin properties from file: ", e);
        } catch (ExecutionException e) {
            log.error("Exception occured while trying to delete topic: ", e);
        } catch (InterruptedException e) {
            log.warn("Operation interrupted: ", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Exception occured while waiting for the topic to be deleted: ", e);
        }
        return success;
    }

    private static String packanizeTopicName(String topicName) {
        if (!topicName.startsWith("pack-")) {
            return "pack-" + topicName;
        }
        return topicName;
    }

    private static boolean isInvalidKafkaTopicName(String name) {
        if (name == null || name.isEmpty()) {
            return true;
        }
        if (name.equals(".") || name.equals("..")) {
            return true;
        }
        if (name.length() > 249) {
            return true;
        }
        return !name.matches("[a-zA-Z0-9._-]+");
    }

    private static Properties loadProperties(String filePath) throws IOException {
        return loadProperties(filePath, new Properties());
    }

    private static Properties loadProperties(String filePath, Properties properties) throws IOException {
        log.trace("Loading properties from file '{}'; extra properties: {}", filePath, properties);
        Properties props = new Properties();
        try (InputStream propsFile = TopicCreator.class.getClassLoader().getResourceAsStream(filePath)) {
            if (propsFile == null) {
                log.error("Cannot find {} in classpath", filePath);
            }
            props.load(propsFile);
            props.putAll(properties);
        }
        return props;
    }

    // not sure how I feel about this and waitUntilTopicDeleted... I am thinking about merging both into one
    // having a bool as third argument called invertRule/doesExist or something
    private static boolean waitUntilTopicExists(AdminClient admin, String topicName) throws ExecutionException, InterruptedException {
        final int timeoutMs = 10_000;
        long start = System.currentTimeMillis();
        long end = start + timeoutMs;

        while (System.currentTimeMillis() < end) {
            if (topicExists(admin, topicName)) {
                return true;
            }
            Thread.sleep(Duration.ofMillis(1000));
        }
        log.error("Timed out while waiting for topic '{}' to appear for " + timeoutMs + " ms", topicName);
        return false;
    }

    private static boolean waitUntilTopicDeleted(AdminClient admin, String topicName) throws Exception {
        final int timeoutMs = 10_000;
        long start = System.currentTimeMillis();
        long end = start + timeoutMs;

        while (System.currentTimeMillis() < end) {
            if (!topicExists(admin, topicName)) {
                return true;
            }
            Thread.sleep(Duration.ofMillis(1000));
        }
        log.error("Timed out while waiting for topic '{}' to be deleted, for " + timeoutMs + " ms", topicName);
        return false;
    }

    private static boolean topicExists(AdminClient admin, String topic) throws ExecutionException, InterruptedException {
        ListTopicsResult topics = admin.listTopics();
        Set<String> names = topics.names().get();
        return names.contains(topic);
    }
}
