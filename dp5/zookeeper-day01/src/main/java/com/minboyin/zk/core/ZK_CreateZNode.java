package com.minboyin.zk.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZK_CreateZNode {

    /**
     * 创建 临时 带 序列编号的  znode
     * @throws Exception
     */
    @Test
    public void testCreateZNode1() throws Exception {
        String path = "/school_name";
        String znodeData = "奈学nx";
        ZooKeeper zk = ZKUtil.getZKConnection(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout);
        zk.create(path, znodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Thread.sleep(10000);
        zk.close();
    }

    /**
     * 创建 临时 不带 序列编号的  znode
     * @throws Exception
     */
    @Test
    public void testCreateZNode2() throws Exception {
        String path = "/school_name";
        String znodeData = "奈学nx";
        ZooKeeper zk = ZKUtil.getZKConnection(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout);
        zk.create(path, znodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create(path, znodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create(path, znodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        Thread.sleep(10000);
        zk.close();
    }

    /**
     * 创建 持久 不带 序列编号的  znode
     * @throws Exception
     */
    @Test
    public void testCreateZNode3() throws Exception {
        String path = "/school_new_name";
        String znodeData = "奈学nx";
        ZooKeeper zk = ZKUtil.getZKConnection(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout);
        zk.create(path, znodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
    }

    /**
     * 创建 持久 带 序列编号的  znode
     * @throws Exception
     */
    @Test
    public void testCreateZNode4() throws Exception {
        String path = "/school/name";
        String znodeData = "奈学NX";
        ZooKeeper zk = ZKUtil.getZKConnection(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout);
        zk.create(path, znodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        zk.close();
    }
}
