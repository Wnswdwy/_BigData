package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        //创建配置对象
        Properties props = new Properties();
        //添加配置
        //指定kafka集群的位置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //指定消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "atguigu-group");
        // 指定自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 提交offset的间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 指定key和value的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //offset重置问题：
        /**
         * auto.offset.reset :
         * 满足两种情况会重置offset:
         *  1. 当启动的消费者之前没有在kafka中有消费记录(新的组新的人)
         *  2. 当启动的消费者要消费的offset在kafka中已经不存在(例如超过7天被删除)
         *
         * 重置到什么位置：
         *  earliest:  目前kafka中topic的分区中最小的offset
         *  latest:    目前kafka中topic的分区中最大的offset
         */

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");



        //创建消费者对象
        KafkaConsumer<String,String>  consumer =
                new KafkaConsumer<String, String>(props);
        //订阅主题
        List<String> topics = new ArrayList<>();
        topics.add("atguigu");
        topics.add("second");
        //topics.add("first");
        consumer.subscribe(topics);


        //消费数据
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic()+" : " + record.partition() +" : " + record.offset() + " : " + record.key() +" : "+ record.value() );
            }
        }

     }

}
