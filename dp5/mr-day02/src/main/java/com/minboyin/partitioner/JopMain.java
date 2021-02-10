package com.minboyin.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/23 20:39
 * 需求：将数据根据电话号码分区
 *  135开头、136开头、137开头、其他
 */

public class JopMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //初始化,创建job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "partitioner");

        //设置Job对象的参数：8个步骤
        //1、设置输入的路径，程序获取源文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D://data/input/flow.log"));

        //2、设置Mapper类型、设置K2、v2类型
        job.setMapperClass(PartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);

        //3、4、5、6是shuffle阶段的：分区、排序、局部合并、分组
        //设置分区类,需要搭配NumReduceTas参数
        job.setPartitionerClass(MyPartitioner.class);
        //设置NumReduceTas参数
        job.setNumReduceTasks(4);

        //7、设置Reducer类型，设置K2、v2类型
        job.setReducerClass(PartitionerReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);

        //输出文件路径存在，则删除该文件夹及文件，谨慎使用
        Path output = new Path("D://data/output_partition");
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
