package com.minboyin.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by minbo on 2021/1/23 20:32
 * Partitioner<KEY, VALUE>
 *     KEY：TEXT，电话号码
 *     VALUE：PartitionBean
 *
 */

public class MyPartitioner extends Partitioner<Text, PartitionBean>{
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int i) {
        if (text.toString().substring(0, 3).equals("135")) {
            return 0;
        }else if (text.toString().substring(0, 3).equals("136")){
            return 1;
        }else if (text.toString().substring(0, 3).equals("137")){
            return 2;
        }else {
            return 3;
        }
    }
}
