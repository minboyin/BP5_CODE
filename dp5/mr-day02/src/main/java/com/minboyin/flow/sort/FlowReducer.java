package com.minboyin.flow.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/24 18:14
 *
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN:FlowBean
 *     VALUEIN:phoneNumber，即 Text
 *     KEYOUT：phoneNumber，即 Text
 *     VALUEOUT：FlowBean
 *
 */

public class FlowReducer extends Reducer<FlowBean,Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历value中的phoneNumber,写入上下文
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
