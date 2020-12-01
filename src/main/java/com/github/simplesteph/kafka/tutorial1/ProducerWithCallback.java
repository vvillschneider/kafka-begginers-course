package com.github.simplesteph.kafka.tutotial1;

import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallback {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    String bootstrapServers = "127.0.0.1:9092";

    // create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties
        .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 0; i < 10; i++) {
      // create a producer record

      ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world " + i);

      // send data
      producer.send(record, (metadata, exception) -> {
        if (Objects.isNull(exception)) {
          logger.info("Received new metadata. Topic: {}, partition: {}, offset: {}, timestamp: {}.",
              metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
        } else {
          logger.error("Error while producing", exception);
        }
      });
    }

    producer.flush();
    producer.close();
  }
}
