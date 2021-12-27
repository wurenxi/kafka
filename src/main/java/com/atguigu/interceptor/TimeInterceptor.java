package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author gxl
 * @description
 * @createDate 2021/12/26 20:40
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 1.取出数据
        String value = record.value();

        // 2.创建一个新的ProducerRecord对象，并返回
        return new ProducerRecord<>(record.topic(),record.partition(), record.key(),
                System.currentTimeMillis()+","+value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }
}
