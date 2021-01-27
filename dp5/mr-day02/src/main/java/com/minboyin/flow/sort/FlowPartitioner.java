package com.minboyin.flow.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by minbo on 2021/1/27 19:09
 */

public class FlowPartitioner extends Partitioner<FlowBean,Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
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
