package com.minboyin.zk.core;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZooKeeper_Watcher {

    private static ZooKeeper zk = null;
    private static String param_path = "/school";

    public static void main(String[] args) throws Exception {

        zk = new ZooKeeper(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent event) {

                Event.EventType type = event.getType();   // 事件类型
                String path = event.getPath();            // 节点路径

                System.out.println(path + "\t-----" + type);

                // 如果需要做循环监听，那么继续在这个响应的地方添加监听
                try {
                    zk.getData(param_path, true, null);  // 注册监听
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // 根据监听的相应的 事件类型 和 节点 这两个信息，可以对应的编写各种业务逻辑
            }
        });

        // 关心： param_path 节点的数据变化
        // 事件： NodeDeleted， NodeDataChanged
        // 触发： delete, setData
        zk.getData(param_path, true, null);    // 注册监听

        // 关心： param_path 子节点个数变化
        // 事件： NodeChildrenChanged, NodeDeleted
        // 触发： create/delete child, delete current
//        zk.getChildren(param_path, true);

        // 关心： 该节点的变化（被创建，被删除，数据被改变）
        // 事件： NodeCreated， NodeDeleted， NodeDataChanged
        // 触发： create,  delete,  setData
//        zk.exists(param_path, true);


        // 主线程执行完毕，但是必须保证 watcer 线程一直工作。所以让主线程一直睡眠。
        Thread.sleep(Long.MAX_VALUE);


        // 如果启动了监听的话，那么就不会关闭zk链接了。
        // zk.close();
    }
}
