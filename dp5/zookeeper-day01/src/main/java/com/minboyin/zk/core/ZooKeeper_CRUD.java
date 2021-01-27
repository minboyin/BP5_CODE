package com.minboyin.zk.core;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class ZooKeeper_CRUD {

    // 获取zookeeper连接时所需要的服务器连接信息，格式为主机名：端口号
    private static final String ConnectString = "bigdata02:2181,bigdata03:2181,bigdata04:2181";

    // 请求链接的会话超时时长
    private static final int SessionTimeout = 5000;

    private static ZooKeeper zk = null;

    @Before
    public void init(){
        try {
            /**
             * 获取zookeeper链接， 要求的连接参数至少有三个：
             * ConnectString:服务器的连接信息
             * SessionTimeout：请求连接的超时时长
             * Watch:添加监听器
             */
            zk = new ZooKeeper(ConnectString, SessionTimeout, null);
        } catch (IOException e) {
            System.out.println("初始化 ZooKeeper 会话链接对象失败 ");
            e.printStackTrace();
        }
    }

    // 创建节点

    // 设置节点数据

    // 查看节点数据

    // 查看节点下的所有子节点列表

    // 删除节点


    // 判断节点是否存在
    public static Stat exists(String path, ZooKeeper zk) throws Exception {
        Stat exists = zk.exists(path, false);
        if (null != exists) {
            System.out.println("该节点" + path + "存在");
            return exists;
        } else {
            System.out.println("该节点" + path + "不存在");
            return null;
        }
    }

    // close()
    @After
    public void close(){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
