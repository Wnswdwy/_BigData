package partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 消息中包含'jiajia'的去往0号分区
 * 消息中包含'fangfang'的去往1号分区
 *
 *
 */
public class MyPartitioner implements Partitioner {

    /**
     * 计算当前消息的分区号
     * @param topic  当前消息发往哪个主题
     * @param key    当前消息的key
     * @param keyBytes 当前消息序列化以后的key
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(value.toString().contains("jiajia")){
            return 0 ;
        }else if(value.toString().contains("fangfang")){
            return 1 ;
        }else{
            return 2 ;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
