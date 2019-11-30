package com.demo;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumerForAssignAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(MyConsumerForAssignAndSeek.class);
        String bootStrapServer = "localhost:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);


        KafkaConsumer<String,String> kafkaConsumer =  new KafkaConsumer<>(properties);

        TopicPartition topicPartition = new TopicPartition(topic,0);
        kafkaConsumer.assign(Arrays.asList(topicPartition));

        kafkaConsumer.seek(topicPartition,200L);


        while (true) {
            ConsumerRecords<String, String> consumerRecords =
                    kafkaConsumer.poll(Duration.ofMillis(3000L));
            consumerRecords.forEach(
                    record -> logger.info("Message Key: " + record.key() + "\n"
                            + " Value: " + record.value() + "\n"
                            + " Partition : " + record.partition() + "\n"
                            + " Offset : " + record.offset() + "\n"
                    ));
        }

    }
}
