package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author gxl
 * @description
 * @createDate 2021/12/26 20:47
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {

    int success;

    int error;

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(recordMetadata != null){
            success++;
        }else{
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success: "+success);
        System.out.println("error: "+error);
    }
}
