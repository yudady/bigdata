package tk.tommy.zookeeper;

import org.apache.zookeeper.ZooKeeper;

public class Zookeeper01 {

    public static void main(String[] args) throws Exception {
        String connStr = "hadoop101:2181,hadoop102:2181,hadoop101:2182";
        int session_time = 5000; // 每5秒发送一次心跳
        ZooKeeper zk =
                new ZooKeeper(
                        connStr,
                        session_time,
                        (event -> {
                            System.out.println(event.getPath());
                            System.out.println(event.getState());
                        }));

        System.out.println("zk status=" + zk.getState());
    }
}
