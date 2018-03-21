package com.luangeng.zookeeper.zkqueue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by LG on 2017/8/19.
 */
public class Producer extends Thread {

    private static AtomicInteger count = new AtomicInteger(0);
    private ZooKeeper zk;
    private Random ran = new Random();

    Producer() throws IOException {
        this.zk = new ZooKeeper("localhost:2181", 30000, null);
    }

    void produce(String str) throws KeeperException, InterruptedException {
        String name = zk.create(ZkQueue.root + "/element", str.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        //System.out.println(getName() + " create: " + str);
    }

    @Override
    public void run() {
        try {
            while (true) {
                String msg = "msg" + count.getAndIncrement();
                produce(msg);
                Thread.sleep(ran.nextInt(1000));
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
        }
    }
}
