package com.minboyin.zk.core;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZKWatch {

    public static void main(String[] args) throws IOException {

        ZooKeeper zk = new ZooKeeper(ZK_VAR.ConnectString, ZK_VAR.SessionTimeout, new Watcher() {

            /**
             * 回调方法
             * @param event
             */
            @Override
            public void process(WatchedEvent event) {

                // 根据 事件类型 和  节点路径 两个参数来决定 业务逻辑

                String path = event.getPath();
                Event.EventType type = event.getType();

                if(path.equals("/name") && type == Event.EventType.NodeCreated){


                }
            }
        });


//        zk.exists(path, true);


    }
}
