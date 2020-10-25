package com.nswdwy.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义UDTF函数需要继承Hive提供的GenericUDTF类,重写相应的方法.
 *
 * 需求: select  myudtf('hadoop,hive,flume,kafka',',') ;
 *
 *       hadoop
 *       hive
 *       flume
 *       kafka
 */
public class StringExplodeUDTF extends GenericUDTF {

    List<String> outList  = new ArrayList<>();

    /**
     *
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //约定返回的列的名字
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");

        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 函数的核心处理方法。
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        //获取传入到函数的第一个参数： 待处理的字符串
        String data = args[0].toString();
        //获取传入到函数的第二个参数:  分隔符
        String split = args[1].toString();
        
        //处理过程
        String[] words = data.split(split);
        //将每个单词作为一行数据写出
        for (String word : words) {
            //先清空集合
            outList.clear();
            //将当前行的数据封装到集合中
            outList.add(word);
            //写出每个单词
            forward(outList);
        }
    }

    /**
     * 通常用于完成一些资源释放，收尾工作等.
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {

    }
}
