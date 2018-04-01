package com.luangeng.zookeeper.zkqueue;

import com.luangeng.zookeeper.ZkUtils;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by LG on 2017/8/19.
 */
public class ZkQueue implements Watcher {

    public static final String root = "/zkqueue";
    private static ZooKeeper zk;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        zk = new ZooKeeper("localhost:2181", 30000, null);

        ZkUtils.delete(zk, "/__locks__/queue");//此处需保证该路径下没有子节点，否则可能获取到为最小

        ZkUtils.delete(zk, root);
        zk.create(root, "queue".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //模拟2个生产者
        new Producer("p1").start();
        new Producer("p2").start();

        //模拟3个消费者
        new Consumer("c1").start();
        new Consumer("c2").start();
        new Consumer("c3").start();

        Scanner s = new Scanner(System.in);
        s.nextLine();

        ZkUtils.delete(zk, root);

        // 关闭连接
        zk.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.printf("---->%s %s\n", watchedEvent.getPath(), watchedEvent.getType());
    }

}
