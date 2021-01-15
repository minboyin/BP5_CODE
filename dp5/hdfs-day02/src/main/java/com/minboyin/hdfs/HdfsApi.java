package com.minboyin.hdfs;

/**
 * Created by minbo on 2021/1/10 12:32
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsApi {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        HdfsApi hdfsApi = new HdfsApi();
        //hdfsApi.getFileSystem1();
        //dfsApi.getFileSystem2();
        //hdfsApi.listFiles();
        hdfsApi.mkdirs();
    }

    /**
     * 获取文件系统
     */

    public void getFileSystem1() throws IOException {
        //1、创建Configuration对象
        Configuration conf = new Configuration();

        //2、设置文件系统类型
        conf.set("fs.defaultFS","hdfs://bigdata01:8020");

        String s1=conf.get("dfs.replication");


        //3、获取指定文件系统
        FileSystem fileSystem = FileSystem.get(conf);

        //4、打印输出测试
        System.out.println(fileSystem);
        System.out.println(s1);
    }

    /**
     * 获取文件系统
     */

    public void getFileSystem2() throws URISyntaxException, IOException {
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://bigdata01:8020"),new Configuration());
        System.out.println("fileSystem"+fileSystem);
    }

    /**
     * 遍历文件夹
     */

    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        //1、获取一个文件系统
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://bigdata01:8020"),new Configuration(),"root");
        //2、调用方法
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        //3、迭代遍历每个文件夹
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            //4、打印输出
            System.out.println(fileStatus.getPath()+"----------");

        }
    }

    /**
     * 创建文件夹
     */
    public void mkdirs() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://bigdata01:8020"),new Configuration(),"root");
        fileSystem.mkdirs(new Path("/test1"));
        fileSystem.close();
    }

}
