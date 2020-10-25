package com.nswdwy.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义UDF函数需要继承GenericUDF类,并重写抽象方法.
 *
 * 实现的效果: my_len("abcd")  -> 4
 *
 */
public class StringLengthUDF extends GenericUDF {
    /**
     * 完成一些初始化工作
     * @param arguments
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //1. 判断参数的个数
        if(arguments.length !=1){
            throw new UDFArgumentLengthException("Input Args Length Error ....");
        }
        //2. 判断参数的类型
        if(!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"Input Args Type Error ....");
        }

        //3. 返回 函数的返回值类型的ObjectInspector

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector ;
    }

    /**
     * 函数处理的核心方法.
     * @param arguments
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 获取参数
        Object arg1 = arguments[0].get();

        if(arg1 == null){
            return 0 ;
        }else{
            return arg1.toString().length();
        }

    }


    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
