package com.nswdwy.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yycstart
 * @create 2020-09-21 14:58
 *
 * 自定义UDTF函数需要继承Hive提供的GenericUDTF类,重写相应的方法.
 *
 * 需求: select  myudtf('hadoop,hive,flume,kafka',',') ;
 *
 *       hadoop
 *       hive
 *       flume
 *       kafka
 *
 *
 */
public class StringUdtfTest extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //约定返回的列的名字
        List<String> list = new ArrayList<>();
        list.add("word");
        //约定返回的列的类型
       List<ObjectInspector> objects = new ArrayList<>();

        return null;
    }

    @Override
    public void process(Object[] objects) throws HiveException {

    }

    @Override
    public void close() throws HiveException {

    }
}
