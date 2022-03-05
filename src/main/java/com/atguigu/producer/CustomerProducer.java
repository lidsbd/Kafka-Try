package com.atguigu.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author lds
 * @date 2021-12-09  15:33
 */
public class CustomerProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建kafka生产者配置对象
        Properties properties = new Properties();
        //2.添加配置信息
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        //key value序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //4.调用send方法,发送消息
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", "kafka" + i));
        }

        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> result = kafkaProducer.send(new ProducerRecord<String, String>("first", "Message" + i, "这是第" + i + "条信息"),
                    //回调函数，当producer发送的数据完成以后，返回告诉producer数据发送成功
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            int partition = metadata.partition();
                            String topic = metadata.topic();
                            long offset = metadata.offset();
                            System.out.println(
                                    topic + "话题"
                                            + partition + "分区"
                                            + offset + "消息发送成功"
                            );
                        }
                    });
            /*
            如下一行代码产生同步回调和异同回调两种方式：
            同步回调：加了此行代码，生产者收到ack以后再发第二条消息；类似打电话
            异步回调：未加此行代码，生成者只要一直发送消息既可。类似发短信
            */
//            RecordMetadata recordMetadata = result.get();

            System.out.println("第" + i + "条消息发送结束");

        }


        //5.关闭资源
        kafkaProducer.close();
    }
}
