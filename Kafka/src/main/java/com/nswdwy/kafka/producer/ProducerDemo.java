package com.nswdwy.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-09-27 18:20
 *
 *
 *   生产者
 *   客户端性质的开发:
 *    1. 创建客户端对象
 *
 *   2. 调方法
 *
 *    3. 关闭对象
 *
 *    配置类:
 *
 *    ProducerConfig ： 生产者相关的配置类
 *    CounsumerConfig:  消费者相关的配置类
 *    CommonClientConfigs: 通用的配置类
 *
 *
 */
public class ProducerDemo {
    public static void main(String[] args) {
        // 创建配置对象
        Properties props = new Properties();

        //kafka集群，broker-list
        //props.put("bootstrap.servers", "hadoop102:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
//        props.put("acks", "all");
//        props.put(ProducerConfig.ACKS_CONFIG,"all");
            props.put(ProducerConfig.ACKS_CONFIG,"all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG,3);
        //批次大小
        //props.put("batch.size", 16384);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //等待时间
//        props.put("linger.ms", 1);
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1);
        props.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //RecordAccumulator缓冲区大小
//        props.put("buffer.memory", 33554432);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        //指定key和value的序列化器
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //创建kafka对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for(int i = 1;i <= 10 ; i++){
            //不带回调函数的
//            producer.send(new ProducerRecord<String,String>("atguigu","producer"+i));
            //带回调函数的
            producer.send(new ProducerRecord<String,String>("atguigu","producer"+i),
                    new  Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null){
                        //消息发送失败
                        System.out.println(e.getMessage());
                    }else{
                        System.out.println(recordMetadata.topic() + " : " + recordMetadata.partition() + " : " + recordMetadata.offset());
                    }
                }
            });
        }
        //关闭对象
        producer.close();
    }
}
