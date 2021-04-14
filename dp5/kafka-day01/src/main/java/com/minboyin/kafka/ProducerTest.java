package com.minboyin.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by minbo on 2021/3/23 20:06
 */

public class ProducerTest {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        //properties.put("bootstrap.servers","bigdata1:9092,bigdata2:9092,bigdata3:9092");
        properties.put(ConsumerConfig.BOOT_STRAP_CONFIG,KafkaServers.KAFKA_SERVERS);

        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");

        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("test", "key6", "message6");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    System.out.println("消息发送成功");
                }else {
                    // 消息发送失败，需要重新发送
                }

            }
        });
        Thread.sleep(10*1000);

        producer.close();
    }
}
