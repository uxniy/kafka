package com.demo;

import java.util.Arrays;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class MyKafkaConsumer {
    Logger logger =  LoggerFactory.getLogger(MyKafkaConsumer.class);
    public static void main(String[] args) {


        new MyKafkaConsumer().run();






    }

    private MyKafkaConsumer(){}


    public void run() {
        String bootStrapServer = "localhost:9092";
        String topic = "first_topic";
        String groupId = "my-third-application";
        int count = 1;
        CountDownLatch latch = new CountDownLatch(count);
        logger.info("Create a new Consumer");

        ConsumerRunnerable myConsumerRunnable = new ConsumerRunnerable(bootStrapServer,topic,groupId,latch);
        new Thread(myConsumerRunnable).start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook.");
            myConsumerRunnable.shutdown();
            try{
                latch.await();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application has exited.");

        }));

        try{
            latch.await();
        }catch (InterruptedException e){
            logger.info("Application got interrupted.");
        }finally {
            logger.info("Application is closing.");
        }

    }

    public class ConsumerRunnerable implements Runnable{

        private KafkaConsumer<String, String> kafkaConsumer;
        private CountDownLatch latch;

        public ConsumerRunnerable(String bootStrapServer, String topic, String groupId, CountDownLatch latch){
            Properties properties = new Properties();
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

            this.kafkaConsumer =  new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Arrays.asList(topic));

            this.latch = latch;

        }


        @Override
        public void run() {
            try{
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
            }catch (WakeupException e){
                logger.info("Recieved shutdown signal");

            }finally {
                kafkaConsumer.close();
                latch.countDown();
            }


        }

        public void shutdown(){
            kafkaConsumer.wakeup();
        }
    }
}
