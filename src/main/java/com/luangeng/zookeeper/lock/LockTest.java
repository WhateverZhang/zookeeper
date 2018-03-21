package com.luangeng.zookeeper.lock;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by LG on 2017/9/28.
 */
public class LockTest extends Thread {


    private static CountDownLatch latch = new CountDownLatch(1);

    private static CountDownLatch latch2 = new CountDownLatch(2000);
    private static int count = 0;
    //private DisLock lock = new DisLock("test");

    public LockTest() throws InterruptedException, IOException, KeeperException {
    }

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        for (int i = 0; i < 2000; i++) {
            LockTest test = new LockTest();
            test.start();
            System.out.println(i);
        }
        latch.countDown();
        latch2.await();
        System.out.println(count);
    }

    public void run() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //for (int i = 0; i < 10; i++) {
        //lock.lock();
        count++;
        count--;
        //lock.unlock();
        //}
        latch2.countDown();
    }
}
