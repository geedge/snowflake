package cn.ac.iie.utils.zookeeper;

import cn.ac.iie.common.FlowWriteConfig;
import org.apache.commons.lang3.RandomUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author qidaijie
 */
public class ZookeeperUtils implements Watcher {
    private static Logger logger = Logger.getLogger(ZookeeperUtils.class);

    private ZooKeeper zookeeper;

    private static final int SESSION_TIME_OUT = 20000;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }
    }


    /**
     * 修改节点信息
     *
     * @param path 节点路径
     */
    public int modifyNode(String path) {
        createNode("/Snowflake", null, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        createNode("/Snowflake/" + FlowWriteConfig.KAFKA_TOPIC, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        int workerId;
        try {
            connectZookeeper();
            Stat stat = zookeeper.exists(path, true);
            workerId = Integer.parseInt(getNodeDate(path));
            if (workerId > 55) {
                workerId = 0;
                zookeeper.setData(path, "1".getBytes(), stat.getVersion());
            } else {
                String result = String.valueOf(workerId + 1);
                if (stat != null) {
                    zookeeper.setData(path, result.getBytes(), stat.getVersion());
                } else {
                    logger.error("Node does not exist!,Can't modify");
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            workerId = RandomUtils.nextInt(56, 63);
        } finally {
            closeConn();
        }
        logger.error("工作ID是：" + workerId);
        return workerId;
    }

    /**
     * 连接zookeeper
     *
     */
    private void connectZookeeper() {
        try {
            zookeeper = new ZooKeeper(FlowWriteConfig.ZOOKEEPER_SERVERS, SESSION_TIME_OUT, this);
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    private void closeConn() {
        try {
            if (zookeeper != null) {
                zookeeper.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取节点内容
     *
     * @param path 节点路径
     * @return 内容/异常null
     */
    private String getNodeDate(String path) {
        String result = null;
        Stat stat = new Stat();
        try {
            byte[] resByte = zookeeper.getData(path, true, stat);
            result = new String(resByte);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Get node information exception");
            e.printStackTrace();
        }
        return result;
    }

    /**
     * @param path 节点创建的路径
     * @param date 节点所存储的数据的byte[]
     * @param acls 控制权限策略
     */
    private void createNode(String path, byte[] date, List<ACL> acls) {
        try {
            connectZookeeper();
            Stat exists = zookeeper.exists(path, true);
            if (exists == null) {
                zookeeper.create(path, date, acls, CreateMode.PERSISTENT);
            } else {
                logger.warn("Node already exists!,Don't need to create");
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            closeConn();
        }
    }

}
