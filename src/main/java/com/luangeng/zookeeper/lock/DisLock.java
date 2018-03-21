package com.luangeng.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 分布式锁
 */
public class DisLock implements Watcher {

    public static final String LOCK_ROOT = "/__locks__";

    private ZooKeeper zk;

    //锁名称,标识竞争的是哪个锁
    private String lockName;

    //当前创建的节点路径
    private String path;

    //前一个节点的路径
    private String prePath;

    //是否获取锁
    private boolean acquired;

    //构造函数，连接zk，检查父节点存在
    public DisLock(String lockName) throws KeeperException, InterruptedException, IOException {
        this.lockName = "/" + lockName;
        this.zk = new ZooKeeper("localhost:2181", 30000, this);
        Objects.nonNull(zk);
        if (zk.exists(LOCK_ROOT, false) == null) {
            zk.create(LOCK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        if (zk.exists(LOCK_ROOT + this.lockName, false) == null) {
            zk.create(LOCK_ROOT + this.lockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
    }

    public void lock() {
        if (tryLock()) {
            return;
        } else {
            waitLock();

        }
    }

    //尝试获取锁
    public boolean tryLock() {
        if (acquired) {
            return true;
        }
        try {
            //创建临时节点，自动编号
            path = zk.create(LOCK_ROOT + lockName + "/", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);

            List<String> ls = zk.getChildren(LOCK_ROOT + lockName, false);
            Collections.sort(ls);
            if (path.equals(LOCK_ROOT + lockName + "/" + ls.get(0))) {
                acquired = true;
                System.out.println(Thread.currentThread().getName() + " get lock");
                return true;
            }

            for (int i = 0; i < ls.size(); i++) {
                if (path.equals(LOCK_ROOT + lockName + "/" + ls.get(i))) {
                    prePath = LOCK_ROOT + lockName + "/" + ls.get(i - 1);
                    break;
                }
            }
            return false;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    //释放锁
    public void unlock() {
        if (!acquired) {
            return;
        }
        try {
            zk.delete(path, -1);
            acquired = false;
            System.out.println(Thread.currentThread().getName() + " free lock");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    //设置监控并等待锁，前一个节点被删除后退出等待，得到锁
    private synchronized void waitLock() {
        try {
            Stat s = zk.exists(prePath, true);
            if (s == null) {
                //等到锁，返回
                acquired = true;
                System.out.println(Thread.currentThread().getName() + " get lock");
                return;
            }
            this.wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        waitLock();
        return;
    }

    //监视节点变化，被监控节点被删除时激活等待锁的线程
    @Override
    public synchronized void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            //System.out.println("触发："+watchedEvent.getPath());
            this.notify();
        }
    }
}
