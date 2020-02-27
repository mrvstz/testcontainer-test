package com.commercetools.webreactive;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ParallelContainerTest {

    private static KafkaContainer kafkaContainer;
    private static KafkaProducer kafkaProducer;

    @BeforeClass
    public static void createKafka() {
        System.out.println("creating kafka image");
        kafkaContainer = new KafkaContainer("4.1.1").withExposedPorts(2181, 9093);
    }

    private void pushToKafka() {
        kafkaContainer.start();
        ImmutableMap<String, String> of = ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"
        );

        kafkaProducer = new KafkaProducer(of);

        kafkaProducer.send(new ProducerRecord<String, String>("my-topic", "test", "wassup"));
    }

    @Test
    public void testKafkaContainer() {
        pushToKafka();
        System.out.println(kafkaContainer.getBootstrapServers());
        ImmutableMap<String, String> earliest = ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"
        );

        KafkaConsumer consumer = new KafkaConsumer(earliest);
        consumer.subscribe(Arrays.asList("my-topic"));

        while (true) {
            ConsumerRecords poll = consumer.poll(Duration.ofSeconds(1));

            if (poll.count() == 1) {
                poll.forEach(o -> System.out.println(o));
                break;
            }
        }
        assert 1 == 1;
    }
}
