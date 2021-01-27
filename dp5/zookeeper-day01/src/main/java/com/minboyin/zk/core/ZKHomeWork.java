package com.minboyin.zk.core;

import java.util.Map;

import org.apache.zookeeper.ZooKeeper;

/**
 * 编程思维训练
 * 1、级联查看某节点下所有节点及节点值
 * 2、删除一个节点，不管有有没有任何子节点
 * 3、级联创建任意节点
 * 4、清空子节点
 */
public interface ZKHomeWork {
	
	/**
	 * 级联查看某节点下所有节点及节点值
	 */
	public Map<String, String> getChildNodeAndValue(String path) throws Exception;

	/**
	 * 删除一个节点，不管有有没有任何子节点
	 */
	public boolean rmr(String path, ZooKeeper zk) throws Exception;

	/**
	 * 级联创建任意节点
	 */
	public boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception;

	/**
	 * 清空子节点
	 */
	public boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception;
	
}
