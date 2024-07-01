package com.demo.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class MyLen extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length!=1){
            throw new UDFArgumentException("只接受一个参数");
        }
        ObjectInspector argument = arguments[0];
        if(ObjectInspector.Category.PRIMITIVE!=argument.getCategory()){
            throw new UDFArgumentException("参数类型不符合");
        }
        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) argument;
        if(PrimitiveObjectInspector.PrimitiveCategory.STRING!=primitiveObjectInspector.getPrimitiveCategory()){
            throw new UDFArgumentException("只能接受String类型的参数");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        DeferredObject argument = arguments[0];
        Object value = argument.get();
        if(value==null){
            return 0;
        }
        return value.toString().length();

    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
