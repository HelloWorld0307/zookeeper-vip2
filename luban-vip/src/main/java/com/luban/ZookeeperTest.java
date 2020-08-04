package com.luban;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZookeeperTest {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 连接服务端，连接地址可以写多个，比如：127.0.0.1：2181，127.0.0.1：2182，127.0.0.1：2183，
        // 当客户端连接服务端失败后就会去循环尝试连接其他的服务地址
        // 初始化timeout,watcher
        // 启动SendThread，这一步包括socket，初始化，读写事件；EventThread
        // new 一个Zookeeper后不socket不一定建立，
//        ZooKeeper client = new ZooKeeper("192.168.204.112:2181", 10000, new Watcher() {
        ZooKeeper client = new ZooKeeper("127.0.0.1:2181", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("默认的watch:" + event.getType());
            }
        }, false);

        // 这一行代码其实就是把命令放入到outgoingQueue中，只有上一步的SendThread启动完成后，才会去取outgoingQueue中的数据去执行
        // zookeeper的数据是同步的，当执行了这条数据后会一直阻塞知道服务端响应结果才会继续往下走
        client.create("/luban", "lb".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat stat = new Stat();

        // 同步请求
        byte[] result = client.getData("/luban",false, stat);

        System.out.println(new String(client.getData("/luban", true, null))+"==========");
//
//
//        // 异步的方式请求，
//        client.getData("/luban", false, new AsyncCallback.DataCallback() {
//            @Override
//            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
//                System.out.println(1);
//            }
//        }, "123");

        System.out.println();
    }
}
