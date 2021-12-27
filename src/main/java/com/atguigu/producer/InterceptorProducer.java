package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author gxl
 * @description
 * @createDate 2021/12/26 20:50
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        // 1.创建Kafka生产者的配置信息
        Properties properties = new Properties();

        // 2.指定连接的kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // 8.key value的序列化类
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "atguigu","atguigu-" + i), (metadata, e) -> {
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
