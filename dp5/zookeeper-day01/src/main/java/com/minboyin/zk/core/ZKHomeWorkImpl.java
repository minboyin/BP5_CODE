package com.minboyin.zk.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.Map;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2017年4月9日 下午11:39:10
 *
 * 编程思维训练
 * 1、级联查看某节点下所有节点及节点值
 * 2、删除一个节点，不管有有没有任何子节点
 * 3、级联创建任意节点
 * 4、清空子节点
 */
public class ZKHomeWorkImpl implements ZKHomeWork {

    @Override
    public Map<String, String> getChildNodeAndValue(String path) throws Exception {
        return null;
    }

    @Override
    public boolean rmr(String path, ZooKeeper zk) throws Exception {
        // 如果方法指定的参数path不存在，那么就可以直接返回
        if (zk.exists(path, null) == null) {
            System.out.println("该路径 " + path + " 不存在");
            return true;
        }

        List<String> children = zk.getChildren(path, null);

        // 表示没有子znode
        if (children.size() == 0) {
            zk.delete(path, -1);
            // 表示有子节点
        } else {
            // 这个for循环 是 删除 child 路径下的所有子节点，但是并不删除 child 节点
            for (String child : children) {
                String deletePath = path + "/" + child;
                rmr(deletePath, zk);
            }
            // 删除完 该节点的所有子节点之后，他们它就可以直接被删除了
            // rmr(path, zk);
            zk.delete(path, -1);
        }

        return true;
    }

    @Override
    public boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception {
        // 首先判断该节点是否存在，如果存在，则不创建， return false
        if (zk.exists(znodePath, null) != null) {
            return false;
        } else {
            try {
                // 直接创建，如果抛异常，则捕捉异常，然后根据对应的异常如果是发现没有父节点，那么就创建父节点
                zk.create(znodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                // 截取父节点
                String parentPath = znodePath.substring(0, znodePath.lastIndexOf("/"));
                // 创建父节点
                createZNode(parentPath, parentPath, zk);
                try {
                    // 父节点创建好了之后，创建该节点
                    zk.create(znodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    @Override
    public boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception {
        List<String> children = zk.getChildren(znodePath, null);
        for (String child : children) {
            String childNode = znodePath + "/" + child;
            if (zk.getChildren(childNode, null).size() != 0) {
                clearChildNode(childNode, zk);
            }
            zk.delete(childNode, -1);
        }
        return true;
    }
}
