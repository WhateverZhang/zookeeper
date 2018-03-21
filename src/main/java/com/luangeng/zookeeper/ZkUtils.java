package com.luangeng.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * Created by LG on 2017/9/23.
 */
public class ZkUtils {

    public static void delete(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            return;
        }
        List<String> ls = zk.getChildren(path, false);
        for (String s : ls) {
            delete(zk, path + "/" + s);
        }
        zk.delete(path, -1);
    }

    public static void deletesub(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            return;
        }
        List<String> ls = zk.getChildren(path, false);
        for (String s : ls) {
            delete(zk, path + "/" + s);
        }
    }
}
