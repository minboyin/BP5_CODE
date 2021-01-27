package com.minboyin.flow.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/24 18:13
 * 1363157995033 	15920133257	5C-0E-8B-C7-BA-20:CMCC	120.197.40.4	sug.so.360.cn	信息安全	   20	     20		    3156	  2936	     200
 * 时间戳		    手机号		基站编号			        IP		        URL		        URL类型	  上行数据包  下行数据包	上行流量   下行流量     响应
 * 0                1           2                                                                  6          7          8         9
 *
 * ** 流量FlowBean的实体类
 *  *     | 上行数据包 | upFlow        | int    |
 *  *     | 下行数据包 | downFlow      | int    |
 *  *     | 上行流量   | upCountFlow   | int    |
 *  *     | 下行流量   | downCountFlow | int    |
 *
 *  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *      KEYIN：数据的偏移量:LongWritable
 *      VALUEIN：一行文本的数据类型呢:Text
 *      KEYOUT：将解析后文本中特定的数据封装到FlowBean类中
 *      VALUEOUT：此需求中是 phoneNumber:Text
 */

public class FlowMapper extends Mapper <LongWritable, Text, FlowBean,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、拆分文本的数据集
        String[] split = value.toString().split("\t");
        String phoneNumber=split[0];

        //2、实例化FlowBean对象，将数据封装到FlowBean中
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));

        //3、写入上下文，由于排序只能对key进行比较，所以keyout为flowBean
        context.write(flowBean,new Text(phoneNumber));
    }
}
