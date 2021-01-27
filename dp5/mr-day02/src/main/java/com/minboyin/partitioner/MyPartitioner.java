package com.minboyin.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by minbo on 2021/1/23 20:32
 * Partitioner<KEY, VALUE>
 *     KEY：单词的类型
 *     VALUE：次数的类型
 *
 */

public class MyPartitioner extends Partitioner<Text, LongWritable>{
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        //分区
        //需求：
        if (text.toString().length() >=5){
            return 0;
        }else{
            return 1;
        }
    }
}
