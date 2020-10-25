package com.nswdwy.kafka.partitioner;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-09-27 19:27
 */
public class ProducerDemo2 {
    public static void main(String[] args) {
        Properties pros = new Properties();
        //bootstrap-server
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //acks
        pros.put(ProducerConfig.ACKS_CONFIG,"all");
        //size 批次大小
        pros.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        //重试次数
        pros.put(ProducerConfig.RETRIES_CONFIG,3);
        //等待时间
        pros.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //key和value
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //指定分区器
        pros.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.nswdwy.kafka.partitioner.Mypratitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);

        for (int i = 1; i <= 10; i++) {
            if(i % 2 == 0){
                producer.send(new ProducerRecord<String,String>("atguigu","jiajia-->"+i));
            }else{
                producer.send(new ProducerRecord<String,String>("atguigu","fangfang-->"+i));
            }
        }
        producer.close();
    }
}
