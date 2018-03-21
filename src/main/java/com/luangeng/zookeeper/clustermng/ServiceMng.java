package com.luangeng.zookeeper.clustermng;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by LG on 2017/9/26.
 */
public class ServiceMng {

    private static final String APPS_PATH = "/__apps__";
    private String serviceName;
    private ZooKeeper zk;
    private CountDownLatch latch = new CountDownLatch(1);
    private List<String> serList;
    private Map<String, String> serMap = new HashMap<String, String>();

    ServiceMng(String serviceName) {
        this.serviceName = serviceName;
    }

    public String register(String address, CallBack callback) throws KeeperException, InterruptedException, IOException {
        if (zk != null) {
            throw new IllegalArgumentException("method should not invoke twice.");
        }

        zk = new ZooKeeper("localhost", 30000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        List list = zk.getChildren(APPS_PATH + "/" + serviceName, true);
                        refresh(list);
                        callback.callback(new ChildrenChangedResult(list, serList));
                        serList = list;
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        latch.await();
        if (zk.exists(APPS_PATH, false) == null) {
            zk.create(APPS_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zk.exists(APPS_PATH + "/" + serviceName, false) == null) {
            zk.create(APPS_PATH + "/" + serviceName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String path = zk.create(APPS_PATH + "/" + serviceName + "/", address.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        List list = zk.getChildren(APPS_PATH + "/" + serviceName, true);
        refresh(list);
        serList = list;
        return path;
    }

    private void refresh(List<String> paths) throws KeeperException, InterruptedException {
        for (String path : paths) {
            byte[] b = zk.getData(APPS_PATH + "/" + serviceName + "/" + path, false, null);
            serMap.put(path, new String(b));
        }
    }

    public String queryLeaderIp(String serviceName) throws KeeperException, InterruptedException {
        List<String> apps = zk.getChildren(APPS_PATH + "/" + serviceName, false);
        if (apps.isEmpty()) {
            return null;
        }
        Collections.sort(apps);
        byte[] data = zk.getData(apps.get(0), false, null);
        return new String(data);
    }

    public String queryRandomServerIp(String serviceName) throws KeeperException, InterruptedException {
        List<String> apps = zk.getChildren(APPS_PATH + "/" + serviceName, false);
        if (apps.isEmpty()) {
            return null;
        }
        Random r = new Random();
        byte[] data = zk.getData(apps.get(r.nextInt(apps.size())), false, null);
        return new String(data);
    }

    public String queryAddress(String path) {
        return serMap.get(path);
    }

    public static class ChildrenChangedResult {
        List<String> up = null;
        List<String> down = null;

        ChildrenChangedResult(List now, List last) {
            up = new LinkedList(now);
            up.removeAll(last);
            down = new LinkedList(last);
            down.removeAll(now);
        }

        public List<String> getUp() {
            return up;
        }

        public List<String> getDown() {
            return down;
        }
    }

}
