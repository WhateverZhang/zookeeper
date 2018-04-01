package com.luangeng.zookeeper.zkqueue;

import com.luangeng.zookeeper.lock.DisLock;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.luangeng.zookeeper.zkqueue.ZkQueue.root;


/**
 * Created by LG on 2017/8/19.
 */
public class Consumer extends Thread {

    private ZooKeeper zk;

    private DisLock lock;

    Consumer(String name) throws IOException, KeeperException, InterruptedException {
        super(name);
        lock = new DisLock("queue");
        this.zk = new ZooKeeper("localhost:2181", 30000, null);
    }

    private boolean consume() throws KeeperException, InterruptedException {
        lock.lock();
        try {
            List<String> list = zk.getChildren(root, true);
            if (list.isEmpty()) {
                return true;
            }
            Collections.sort(list);
            String first = list.get(0);
            byte[] b = zk.getData(root + "/" + first, false, null);
            String str = new String(b);
            System.out.println(getName() + " get:" + str);
            zk.delete(root + "/" + first, -1);
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (consume()) {
                    Thread.sleep(10);
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
        }
    }
}
