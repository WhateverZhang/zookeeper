package com.luangeng.zookeeper.clustermng;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by LG on 2017/9/26.
 */
public class ServerUnit {

    public static final String SER_NAME = "ServerUnit";

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        System.out.println("begin register to Zookeeper..");

        String address = "localhost" + ":" + new Random().nextInt(255);

        ServiceMng mng = new ServiceMng(SER_NAME);

        String serverId = mng.register(address, new CallBack<ServiceMng.ChildrenChangedResult>() {
            @Override
            public void callback(ServiceMng.ChildrenChangedResult cn) throws KeeperException, InterruptedException {
                for (String str : cn.getUp()) {
                    System.out.println("检测到服务加入: " + mng.queryAddress(str));
                }
                for (String str : cn.getDown()) {
                    System.out.println("检测到服务退出: " + mng.queryAddress(str));
                }
            }
        });

        System.out.println("ServerUnit started at: " + address);
        TimeUnit.HOURS.sleep(1);
    }
}
