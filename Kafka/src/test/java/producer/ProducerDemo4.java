package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
public class ProducerDemo4 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
           //异步发送
           producer.send(
                   new ProducerRecord<String, String>("atguigu", "message==>" + i),

                   new Callback() {
                       @Override
                       public void onCompletion(RecordMetadata metadata, Exception exception) {
                           System.out.println("消息发送完成......");
                           if(exception !=null){
                               System.out.println(exception.getMessage());
                           }else{
                               System.out.println(metadata.topic()+" : " + metadata.partition()+" : " + metadata.offset());
                           }
                       }
                   }).get();  //调用get方法会阻塞当前线程，直到get拿到结果.

            System.out.println("消息发送出去......");

        }

        //关闭对象
        producer.close();
    }
}
