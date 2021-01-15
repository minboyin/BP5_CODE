package com.minboyin.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by minbo on 2021/1/13 23:52
 */

public class HdfsApi {
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsApi hdfsApi = new HdfsApi();
        hdfsApi.readConf();
    }
    public void readConf() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        String s1=conf.get("dfs.replication");
        conf.set("dfs.replication","3");
        conf.addResource(new Path("C://Users//minbo//IdeaProjects//dp5//hdfs-day01//src//main//resources//hdfs-site.xml"));
        System.out.println(s1);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata01:8020"), conf, "root");
        System.out.println(fileSystem.getConf().get("dfs.replication"));
    }
}
