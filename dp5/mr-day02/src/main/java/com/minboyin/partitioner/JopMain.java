package com.minboyin.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by minbo on 2021/1/23 20:39
 * 需求：根据不同长度的单词
 */

public class JopMain {
    public static void main(String[] args) throws IOException {
        //初始化
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "partitioner");
        //设置Job相关信息：8个小步骤
        //设置输入路径；

        // 设置Mapper类型，并设置k2、v2类型；
        // 3456 为shuffle阶段；
        //

        // 设置reduce类型，并设置k3、v3的类型；

        //设置reduceTask的个数

        // 设置输出路径

        //等待完成
    }
}
