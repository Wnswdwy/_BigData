package com.nswdwy.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Map;

/**
 * @author yycstart
 * @create 2020-09-27 19:23
 */
public class Mypratitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        if(o1.toString().contains("fangfang")){
            return 0;
        }else if(o1.toString().contains("jiajia")){
            return 1;
        }else{
            return 2;
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
