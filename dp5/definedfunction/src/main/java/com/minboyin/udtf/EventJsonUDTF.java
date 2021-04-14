package com.minboyin.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

/**
 * Created by minbo on 2021/3/17 13:30
 */

public class EventJsonUDTF extends GenericUDTF {
    /**
     * 指定：输出参数的名称、输出参数的类型
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1、初始化数据结构
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

        //2、
        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //3、
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    @Override
    public void process(Object[] objects) throws HiveException {

        // 获取传输的数据：et
        String input = objects[0].toString();

        // 对数据进行处理：
        if (StringUtils.isBlank(input)){

            return;

        }else {
            try {

                // 获取共计的事件（ad/facoriters）
                JSONArray ja = new JSONArray(input);

                //
                if (ja == null){
                    return;
                }

                //遍历整个事件
                for (int i =0 ; i < ja.length();i++){

                    String[] result = new String[2];

                    try {
                        // 取出每一个的事件名称（ad/facoriters）
                        result[0]=ja.getJSONObject(i).getString("en");

                        // 取出每个事件整体
                        result[1]=ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }

                    // 结果返回
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
    //没有记录处理的时候该方法会被调用，清理使用
    @Override
    public void close() throws HiveException {

    }
}
