package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author gxl
 * @description
 * @createDate 2021/12/26 17:52
 */
public class CallBackProducer {
    public static void main(String[] args) {
        // 1.创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        // 2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("aaa", "atguigu","atguigu-" + i), (metadata, e) -> {
                if(e == null){
                    System.out.println(metadata.partition()+"--"+metadata.offset());
                }else{
                    e.printStackTrace();
                }
            });
        }

        // 4.关闭资源
        producer.close();
    }
}
