package com.atguigu.producer;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author lds
 * @date 2021-12-10  22:10
 */
public class MyKafkaConsumer {
    public static void main(String[] args) {
        //创建消费者
        Properties properties = new Properties();
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "group10");
        properties.setProperty("auto.offset.reset", "earliest");
        //自动提交offset
        properties.setProperty("enable.auto.commit","false");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //TODO 消费
        ArrayList<String> list = new ArrayList<>();
        list.add("first");
        kafkaConsumer.subscribe(list);
        Duration duration = Duration.ofMillis(5000);
        //拉取数据
        while (true) {
            System.out.println("second commit");
            ConsumerRecords<String, String>  records = kafkaConsumer.poll(duration);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }

    }
}
