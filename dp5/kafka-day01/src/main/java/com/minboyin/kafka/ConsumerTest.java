package com.minboyin.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by minbo on 2021/3/23 21:17
 */

public class ConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();

        //props.put(ProducerConfig.BOOT_STRAP_CONFIG,KafkaServers.KAFKA_SERVERS);
        props.put("bootstrap.servers","bigdata1:9092,bigdata2:9092,bigdata3:9092");

        //指定消费者组
        String groupID = "GroupId1";

        props.put("group.id",groupID);

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset", "earliest");

        String topicName = "test";

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(1000);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.offset()+","+record.key()+","+record.value());
                }
            }
        }catch (Exception e){

        }
    }
}
