package com.minboyin.zk.core;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2017年04月27日上午7:57:17
 * 描述： ZooKeeper API 测试 Demo
 */
public class ZooKeeper_API {

    private ZooKeeper zk;

    /**
     * 获取ZooKeeper链接
     */
    @Before
    public void getZooKeeperConnection() throws Exception {
        zk = new ZooKeeper(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout, null);
    }

    /**
     * 查询某节点下节点列表
     */
    @Test
    public void getZNodeChildrens() {
        try {
            String currentPath = "/name";
            List<String> children = zk.getChildren(currentPath, false);
            for(String child : children){
                String childPath = currentPath + "/" + child;
                byte[] data = zk.getData(childPath, false, null);
                System.out.println(new String(data));
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建持久节点
     */
    @Test
    public void createZKNode_P() {

    }

    /**
     * 创建临时节点
     */
    @Test
    public void createZKNode_E() {

    }

    /**
     * 修改节点数据
     */
    @Test
    public void setData() {

    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void exists() throws Exception {
        String path = "/";
        Stat exists = zk.exists(path, false);
        if (null != exists) {
            System.out.println("该节点 " + path + " 存在");
        } else {
            System.out.println("该节点 " + path + " 不存在");
        }
    }

    /**
     * 删除节点
     */
    @Test
    public void deleteZNode() {

    }

    /**
     * 关闭ZooKeeper链接
     */
    @After
    public void closeZooKeeperConnection() throws Exception {

        zk.close();
    }
}
