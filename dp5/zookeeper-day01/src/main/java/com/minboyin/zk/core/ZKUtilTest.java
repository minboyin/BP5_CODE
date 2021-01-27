/**
 *
 */
package com.minboyin.zk.core;

import java.util.List;

//import com.mazh.nx.zookeeper.core.ZKUtil;
import org.apache.zookeeper.ZooKeeper;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2017年4月9日 上午8:12:48 
 *
 * 描述: ZooKeeper API 测试类
 */
public class ZKUtilTest {

    public static void main(String[] args) throws Exception {

        ZooKeeper zk = ZKUtil.getZKConnection(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout);

        String path = "/school";
        String value = "奈学";

        // 创建节点
        String createZKNode = ZKUtil.createZKNode(path, value, zk);
        System.out.println(createZKNode + "\t" + createZKNode != null ? "创建节点成功" : "创建节点失败");

        // 获取节点数据
        System.out.println(ZKUtil.getZNodeData(path, zk));

        // 判断节点存在不存在
        System.out.println(ZKUtil.existsZNode(path, zk));

        // 获取子节点列表
        List<String> childrenZNodes = ZKUtil.getChildrenZNodes(path, zk);
        for (String znodePath : childrenZNodes) {
            System.out.println(znodePath);
        }

        // 修改节点数据
        System.out.println(ZKUtil.updateZNodeData(path, "奈学NX", zk));
        System.out.println("获取到节点数据：" + ZKUtil.getZNodeData(path, zk));

        // 删除节点
        boolean success = ZKUtil.deleteZNode(path, zk);
        System.out.println(success ? "删除节点成功" : "删除节点失败");
    }
}
