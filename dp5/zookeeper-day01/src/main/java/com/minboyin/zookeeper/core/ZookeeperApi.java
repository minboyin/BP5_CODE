package com.minboyin.zookeeper.core;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * Created by minbo on 2021/1/28 10:04
 */

public class ZookeeperApi {

    //1、节点名称，统一命名
    private static String znode = "/zk";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //创建zookeeper对象
        ZooKeeper zooKeeper = new ZooKeeper(ZK_VAL.ConnectString, ZK_VAL.SessionTimeout, null);

        /**
         * 查询某节点下节点列表
         */
        List<String> children = zooKeeper.getChildren("/", null);
        //将节点列表输出
        for (String child : children) {
            System.out.println(child);
        }

        /**
         * 创建持久节点
         */

        /**
         * 创建临时节点
         */

        /**
         * 修改节点数据
         */

        /**
         * 判断节点是否存在
         */

        /**
         * 删除节点
         */

        /**
         * 关闭ZooKeeper链接
         */

    }

}
