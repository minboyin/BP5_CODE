package com.minboyin.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/23 20:19
 *  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *      KEYIN：数据的偏移量:LongWritable
 *      VALUEIN：一行文本的数据类型呢:Text
 *      KEYOUT：此需求中是 phoneNumber:Text
 *      VALUEOUT：将解析后文本中特定的数据封装到PartitionBean类中
 */

public class PartitionerMapper extends Mapper<LongWritable, Text, Text,PartitionBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、拆分文本的数据集
        String[] split = value.toString().split("\t");
        String phoneNumber=split[1];

        //2、实例化PartitionBean对象，将数据封装到PartitionBean中
        PartitionBean partitionBean = new PartitionBean();
        partitionBean.setUpFlow(Integer.parseInt(split[6]));
        partitionBean.setDownFlow(Integer.parseInt(split[7]));
        partitionBean.setUpCountFlow(Integer.parseInt(split[8]));
        partitionBean.setDownCountFlow(Integer.parseInt(split[9]));

        //3、写入上下文
        context.write(new Text(phoneNumber),partitionBean);

    }
}
