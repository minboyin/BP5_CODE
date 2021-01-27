package com.minboyin.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/23 20:19
 *  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *      KEYIN
 *      VALUEIN
 *      KEYOUT
 *      VALUEOUT
 */

public class WordMapper extends Mapper<LongWritable, Text, Text,LongWritable> {
    // 这一步不会


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
