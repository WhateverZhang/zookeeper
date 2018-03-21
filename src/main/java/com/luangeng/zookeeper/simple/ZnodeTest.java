package com.luangeng.zookeeper.simple;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by LG on 2017/8/18.
 */
public class ZnodeTest {

    // 会话超时时间
    private static final int SESSION_TIMEOUT = 30000;

    // 创建 ZooKeeper 实例
    ZooKeeper zk;

    // 创建 Watcher 实例
    Watcher wh = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("监控到：" + event.getPath() + " " + event.getType() + "  " + event.toString());
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZnodeTest dm = new ZnodeTest();
        dm.createZKInstance();
        dm.ZKOperations();
        dm.ZKClose();
    }

    // 初始化 ZooKeeper 实例
    private void createZKInstance() throws IOException {
        zk = new ZooKeeper("localhost", ZnodeTest.SESSION_TIMEOUT, this.wh);
    }

    private void ZKOperations() throws IOException, InterruptedException, KeeperException {
        rDelete("/zoo");
        System.out.println("创建节点/zoo");
        zk.create("/zoo", "zoo data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println("查看是否创建成功,打开watch： " + zk.exists("/zoo", true));

        System.out.println("修改节点数据,触发watch");
        zk.setData("/zoo", "zoo data update".getBytes(), -1);

        System.out.println("查看是否修改成功，打开watch： " + new String(zk.getData("/zoo", true, null)));

        System.out.println("创建子节点，触发watch");
        zk.create("/zoo/", "keep data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        zk.create("/zoo/keep", "keep data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        zk.create("/zoo/keep", "keep data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        zk.exists("/zoo/keeper", true);
        String keep = zk.create("/zoo/keeper", "keeper data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        System.out.println("查看子节点,打开watch");
        List<String> keeps = zk.getChildren("/zoo", false);
        for (String s : keeps) {
            System.out.println("keeps:" + s);
        }
        zk.getData(keep, true, null);

        System.out.println("删除节点，触发watch");
        zk.delete(keep, -1);

        System.out.println("查看节点是否被删除： " + zk.exists("/zoo", false));

        //rDelete("/zoo");
    }

    private void rDelete(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            return;
        }
        List<String> ls = zk.getChildren(path, false);
        for (String s : ls) {
            rDelete(path + "/" + s);
        }
        zk.delete(path, -1);
    }

    private void ZKClose() throws InterruptedException {
        zk.close();
    }

}
