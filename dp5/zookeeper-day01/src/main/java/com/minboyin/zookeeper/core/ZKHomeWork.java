package com.minboyin.zookeeper.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by minbo on 2021/2/11 10:15
 *
 *  * 1、级联查看某节点下所有节点及节点值：getChildNodeAndValue
 *  * 2、删除一个节点，不管有有没有任何子节点：rmr,需要先将子节点删除，再删除指定节点
 *  * 3、级联创建任意节点：createZNode
 *  * 4、清空子节点：清空子节点
 */

public class ZKHomeWork implements com.minboyin.zk.core.ZKHomeWork {
    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(ZK_VAL.ConnectString, ZK_VAL.SessionTimeout, null);
        ZKHomeWork zkHomeWork = new ZKHomeWork();
        //zkHomeWork.rmr("/zk",zooKeeper);
        //zkHomeWork.createZNode("/zk/znode","niuyear",zooKeeper);
        //zkHomeWork.clearChildNode("/zk",zooKeeper);
        Map<String, String>  result = zkHomeWork.getChildNodeAndValue("/zk");
        System.out.println(result);

    }
    /**
     * 级联查看某节点下所有节点及节点值
     */
    @Override
    public Map<String, String> getChildNodeAndValue(String path) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(ZK_VAL.ConnectString, ZK_VAL.SessionTimeout, null);
        Map<String,String> m = new HashMap<>();
        //判断path是否存在
        if (zooKeeper.exists(path,null)== null){
            System.out.println("该路径 " + path + " 不存在");}

        List<String> children = zooKeeper.getChildren(path, null);

        for (String child : children) {
            String childPath = path + "/" + child;
            if (zooKeeper.getChildren(childPath,null)!=null){
                getChildNodeAndValue(childPath);
            }
            byte[] data = zooKeeper.getData(childPath,null,null);
            m.put(child,new String(data));
        }
        return m;
    }

    /**
     * 删除一个节点，不管有有没有任何子节点，如果有子节点，需要递归删除
     */
    @Override
    public boolean rmr(String path, ZooKeeper zk) throws Exception {
        if (zk.exists(path,null)== null){
            System.out.println("该路径 " + path + " 不存在");
            return true;
        }else {
            List<String> childrens=zk.getChildren(path,null);
            //没有子节点
            if (childrens.size()==0){
                zk.delete(path,-1);
            }
            else {
                // 这个for循环 是 删除 children 路径下的所有子节点，但是并不删除 children 节点
                for (String children : childrens) {
                    String deletePath = path + "/" + children;
                    rmr(deletePath,zk);
                }
                // 删除完 该节点的所有子节点之后，他们它就可以直接被删除了
                // rmr(path, zk);
                zk.delete(path,-1);
            }
        }
        return true;
    }

    /**
     * 级联创建任意节点
     */
    @Override
    public boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception {
        // 作用一：判断节点是否存在，如果节点存在，就不创建
        // 作用二：递归调用createZNode创建父节点，递归出口
        if (zk.exists(znodePath,null)!=null){
            return false;
        }else {
            try {
                // 直接创建，如果有异常，就捕获异常
                zk.create(znodePath,data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }catch (KeeperException e){
                // 捕获异常，根据异常中的信息，截取父节点信息
                String parentPath = znodePath.substring(0,znodePath.lastIndexOf("/"));
                // 递归调用createZNode创建父节点
                createZNode(parentPath,parentPath,zk);
                try {
                    // 父节点创建好了之后，创建该节点
                    zk.create(znodePath,data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }catch (KeeperException e1){
                    e1.printStackTrace();
                }catch (InterruptedException e1){
                    e1.printStackTrace();
                }
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        return true;
    }

    /**
     * 清空子节点，递归调用clearChildNode，将子节点删除
     */
    @Override
    public boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception {
        List<String> children = zk.getChildren(znodePath, null);
        for (String child : children) {
            String childNode = znodePath + "/" + child;
            //递归获取子节点的节点列表
            if (zk.getChildren(childNode,null).size() != 0){
                clearChildNode(child,zk);
            }
            zk.delete(childNode, -1);
        }
        return true;
    }
}
