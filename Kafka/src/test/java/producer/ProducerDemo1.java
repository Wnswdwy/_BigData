package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者
 *
 * 客户端性质的开发:
 *  1. 创建客户端对象
 *
 *  2. 调方法
 *
 *  3. 关闭对象
 *
 *  配置类:
 *
 *  ProducerConfig ： 生产者相关的配置类
 *  CounsumerConfig:  消费者相关的配置类
 *  CommonClientConfigs: 通用的配置类
 *
 *
 */
public class ProducerDemo1 {

    public static void main(String[] args) {
        // 创建配置对象
        Properties props = new Properties();
        // 添加配置
        //kafka集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // ack级别
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


        //创建生产者对象
        KafkaProducer<String, String> producer
                = new KafkaProducer<String, String>(props);

        //生产数据
        for (int i = 1; i <= 10 ; i++) {
            // 粘性分区
            //producer.send(new ProducerRecord<String,String>("atguigu","message==>"+i));

            //指定key  按照key的hash对分区数取余

            // producer.send(new ProducerRecord<String,String>("atguigu","key"+i,"message==>"+i));

            //指定分区号

            producer.send(new ProducerRecord<String,String>("atguigu", 0,"key"+i,"message++>"+i));


        }

        //关闭对象
        producer.close();
    }
}
