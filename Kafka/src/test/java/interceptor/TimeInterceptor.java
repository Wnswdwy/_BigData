package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 在消息内容的前面添加 时间戳
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //获取消息的value
        String value = record.value();
        value = System.currentTimeMillis() +" >> " + value ;

        //封装新的消息
        ProducerRecord<String,String> newRecord  =
                new ProducerRecord<String,String>(
                        record.topic(),
                        record.partition(),
                        record.key(),
                        value);


        return  newRecord;
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
