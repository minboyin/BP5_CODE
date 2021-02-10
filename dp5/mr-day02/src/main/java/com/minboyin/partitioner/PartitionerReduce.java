package com.minboyin.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/23 20:27
 *  * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *  *     KEYIN:phoneNumber，即 Text
 *  *     VALUEIN:PartitionBean
 *  *     KEYOUT：phoneNumber，即 Text
 *  *     VALUEOUT：PartitionBean
 */

public class PartitionerReduce extends Reducer <Text, PartitionBean, Text, PartitionBean>{
    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {
//        for (PartitionBean value : values) {
//            context.write(key,value);
//        }
        //方式二：values.iterator().next()
        while (values.iterator().hasNext()){
            context.write(key,values.iterator().next());
        }
    }
}
