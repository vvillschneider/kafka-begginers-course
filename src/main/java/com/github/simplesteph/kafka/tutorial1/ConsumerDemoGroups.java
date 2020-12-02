package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "kafka-beginners-course-application-2";
    String topic = "first_topic";

    // create Consumer properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties
        .setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create the consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

    // subscribe consume to our topic(s)
    consumer.subscribe(Collections.singleton(topic));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      records.forEach(record -> logger
          .info("Key: {}, value: {}, partition: {}, offset: {}.", record.key(), record.value(),
              record.partition(), record.offset()));
    }
  }
}
