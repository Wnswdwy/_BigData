package com.nswdwy.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author yycstart
 * @create 2020-09-21 14:57
 */
public class StringUdtfTest extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        /**
         * 判断传入参数
         */
        if(objectInspectors.length!=1){
            throw new UDFArgumentException("Input Args Length Error ....");
        }
        if(!objectInspectors[0].equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"Input Args Type Error ....");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 核心处理
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object result = deferredObjects[0].get();
        if(result == null){
            return 0;
        }else{
            return result.toString().length();
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
