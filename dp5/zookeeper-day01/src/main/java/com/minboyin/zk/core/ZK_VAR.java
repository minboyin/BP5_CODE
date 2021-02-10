package com.minboyin.zk.core;

public class ZK_VAR {

    // 获取zookeeper连接时所需要的服务器连接信息，格式为主机名：端口号
    public static final String ConnectString = "bigdata1:2181,bigdata2:2181,bigdata3:2181";

    // 请求了解的会话超时时长
    public static final int SessionTimeout = 5000;
}
