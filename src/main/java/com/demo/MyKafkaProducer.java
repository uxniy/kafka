package com.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) {
        String bootStrapServer = "localhost:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        int i = 0;
        try{
            while(true) {
                i++;
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic,"key_" + i, "hello world " + i);
                kafkaProducer.send(producerRecord);
                kafkaProducer.flush();
                Thread.sleep(1000l);
            }
        }catch (InterruptedException e){
            kafkaProducer.close();
        }finally {
            kafkaProducer.close();
        }





    }
}
