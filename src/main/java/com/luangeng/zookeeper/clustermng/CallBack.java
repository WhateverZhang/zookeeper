package com.luangeng.zookeeper.clustermng;

/**
 * Created by LG on 2017/9/26.
 */
public interface CallBack<T> {

    void callback(T t) throws Exception;

}
