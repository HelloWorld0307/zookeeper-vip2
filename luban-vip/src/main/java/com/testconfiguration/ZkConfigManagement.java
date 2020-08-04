package com.testconfiguration;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;

public class ZkConfigManagement {
    private Config config;

    /**
     * 从数据库下载下来配置项
     * @return Config
     */
    public Config downloadConfigFromDB(){
        config = new Config("test","test");
        return config;
    }

    /**
     * 模拟将配置文件上传到数据库
     * @param nm
     * @param pwd
     */
    public void upLoadConfigToDB(String nm, String pwd){
        if(config == null)
            config = new Config();
        config.setUserNm(nm);
        config.setUserPw(pwd);
    }

    public void syncConfigToZK() throws IOException {
        ZooKeeper client = new ZooKeeper("127.0.0.1:2181", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("默认的watch:" + event.getType());
            }
        }, false);
        // 判断是否存在/zkConfig 节点
        // 监听该节点的状态

    }
}
