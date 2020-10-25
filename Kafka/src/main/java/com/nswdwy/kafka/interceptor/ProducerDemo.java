package com.nswdwy.kafka.interceptor;




import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-09-28 18:38
 */
public class ProducerDemo {
    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG,3);

        //批次大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);

        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG,1);

        //RecordAccumulator缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432); //32M

        //指定key和value的序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //指定拦截器
        List list = new ArrayList();
        list.add("com.nswdwy.kafka.interceptor.TimeInterceptor");
        list.add("com.nswdwy.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 1; i <= 10 ; i++) {
            producer.send(new ProducerRecord<String,String>("atguigu","message"+i),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if(exception != null){
                                System.out.println(exception.getMessage());
                            }else{
                                System.out.println(metadata.topic() + " : " + metadata.partition() + " : "+ metadata.offset());
                            }
                        }
                    });
        }
        //关闭对象
        producer.close();
    }
}
