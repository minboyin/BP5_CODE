package com.minboyin.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/23 20:27
 */

public class WordReduce extends Reducer <Text, LongWritable, Text, LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //
        long count = 0;

//        for(LongWritable );
    }
}
