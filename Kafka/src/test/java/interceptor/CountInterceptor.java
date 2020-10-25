package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 统计发送成功和失败的消息个数
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {
    private Integer success =0 ;
    private Integer fail =0 ;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            //发送失败
            fail ++ ;
        }else{
            //发送成功
            success ++ ;
        }
    }

    @Override
    public void close() {
        System.out.println("SUCCESS:" +success);
        System.out.println("FAIL:" +fail);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
