package org.apache.zookeeper;

import java.util.concurrent.CountDownLatch;

/**
 * @Auther: cheng
 * @Date: 2020/4/8 12:07
 * @Description:
 */
public class Test {
    @org.junit.Test
    public void test1(){
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 10000, new Watcher() {
                public void process(WatchedEvent event) {
//                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
            //客户端调用create在集群内创建node，返回成功创建的路径
//            zooKeeper.create("/test","".getBytes(),
//                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//            //同样存在同步和异步
//            zooKeeper.delete("/test",-1);
//            //异步，回调函数中触发事件观察者
//            zooKeeper.exists("/test",false);
//            zooKeeper.getData("/test",false,null);;
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
