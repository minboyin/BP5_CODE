package com.minboyin.flow.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/24 18:14;21点19分
 */

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"flowSum");

        //设置Job对象的参数：8个步骤
        //1、设置输入的路径，程序获取源文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D://data/input/flow.log"));

        //2、设置Mapper类型、设置K2、v2类型
        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //3、4、5、6是shuffle阶段的：分区、排序、局部合并、分组
        //设置分区类,需要搭配NumReduceTas参数
        //job.setPartitionerClass(FlowPartitioner.class);
        //设置NumReduceTas参数
        //job.setNumReduceTasks(4);

        //7、设置Reducer类型，设置K2、v2类型
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //输出文件路径存在，删除
        Path output = new Path("D://data/output");
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }

        //8、设置输出的路径，需要是一个未存在的文件夹
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,output);

        //等待程序完成，并且输出成功标志
        boolean b=job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);

    }
}
