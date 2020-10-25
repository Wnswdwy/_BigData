package com.nswdwy.kafka.interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.Map;

/**
 * @author yycstart
 * @create 2020-09-28 18:21
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {
    private Integer Success = 0;
    private Integer Fail = 0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null){
            Success++;
        }else{
            Fail++;
        }
    }

    @Override
    public void close() {
        System.out.println("Success: " + Success );
        System.out.println("Fail: " + Fail);

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
