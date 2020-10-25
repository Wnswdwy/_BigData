package com.nswdwy.hive.etl;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-22 13:41
 */
public class EtlMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text result = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String s1 = EtlUtil.eDail(s);

        if(s1 == null){
            return ;
        }


        result.set(s1);
        context.write(result,NullWritable.get());
    }
}
