package com.nswdwy.kafka.interceptor;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author yycstart
 * @create 2020-09-28 18:21
 *
 * 统计成功和失败的次数
 */
public class TimeInterceptor implements ProducerInterceptor<String,String>{

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String val = record.value();
        val = System.currentTimeMillis() + "==>" +val;

        ProducerRecord<String,String> newRecord = new ProducerRecord<>(record.topic(),
                record.partition(),record.key(),val);
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
