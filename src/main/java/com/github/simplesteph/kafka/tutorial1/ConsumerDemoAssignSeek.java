package com.github.simplesteph.kafka.tutorial1;

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

public class ConsumerDemoAssignSeek {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    String bootstrapServers = "127.0.0.1:9092";
    String topic = "first_topic";

    // create Consumer properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties
        .setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create the consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

    // assign
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    long offsetToReadFrom = 7L;
    consumer.assign(Arrays.asList(topicPartition));

    // seek
    consumer.seek(topicPartition, offsetToReadFrom);

    int numberOfMessagedToRead = 5;
    boolean keepOnReading = true;
    int numberOfMessagesReadSoFar = 0;

    // poll for new data
    while (keepOnReading) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        numberOfMessagesReadSoFar += 1;
        logger.info("Key: {}, value: {}, partition: {}, offset: {}.", record.key(), record.value(),
            record.partition(), record.offset());
        if(numberOfMessagesReadSoFar >= numberOfMessagedToRead){
          keepOnReading = false;
          break;
        }
      }
    }
  }
}
