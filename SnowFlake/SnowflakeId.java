package cn.ac.iie.utils.system;

import cn.ac.iie.common.FlowWriteConfig;
import cn.ac.iie.utils.zookeeper.DistributedLock;
import cn.ac.iie.utils.zookeeper.ZookeeperUtils;
import org.apache.log4j.Logger;

/**
 * 雪花算法
 *
 * @author qidaijie
 */
public class SnowflakeId {
    private static Logger logger = Logger.getLogger(SnowflakeId.class);

    // ==============================Fields===========================================
    /**
     * 开始时间截 (2018-08-01 00:00:00) max 17years
     */
    private final long twepoch = 1564588800000L;

    /**
     * 机器id所占的位数
     */
    private final long workerIdBits = 6L;

    /**
     * 数据标识id所占的位数
     */
    private final long dataCenterIdBits = 4L;

    /**
     * 支持的最大机器id，结果是3 (这个移位算法可以很快的计算出几位二进制数所能表示的最大十进制数)
     */
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);

    /**
     * 支持的最大数据标识id，结果是15
     */
    private final long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);

    /**
     * 序列在id中占的位数
     */
    private final long sequenceBits = 14L;

    /**
     * 机器ID向左移12位
     */
    private final long workerIdShift = sequenceBits;

    /**
     * 数据标识id向左移17位(14+6)
     */
    private final long dataCenterIdShift = sequenceBits + workerIdBits;

    /**
     * 时间截向左移22位(4+6+14)
     */
    private final long timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits;

    /**
     * 生成序列的掩码，这里为16383
     */
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    /**
     * 工作机器ID(0~63)
     */
    private long workerId;

    /**
     * 数据中心ID(0~15)
     */
    private long dataCenterId;

    /**
     * 毫秒内序列(0~16383)
     */
    private long sequence = 0L;

    /**
     * 上次生成ID的时间截
     */
    private long lastTimestamp = -1L;


    private static SnowflakeId idWorker;

    private static ZookeeperUtils zookeeperUtils = new ZookeeperUtils();

    static {
        idWorker = new SnowflakeId();
    }

    //==============================Constructors=====================================

    /**
     * 构造函数
     */
    private SnowflakeId() {
        DistributedLock lock = new DistributedLock(FlowWriteConfig.ZOOKEEPER_SERVERS, "disLocks1");
        lock.lock();
        int tmpWorkerId = zookeeperUtils.modifyNode("/Snowflake/" + FlowWriteConfig.KAFKA_TOPIC);
        if (tmpWorkerId > maxWorkerId || tmpWorkerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        if (FlowWriteConfig.DATA_CENTER_ID_NUM > maxDataCenterId || FlowWriteConfig.DATA_CENTER_ID_NUM < 0) {
            throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDataCenterId));
        }
        this.workerId = tmpWorkerId;
        this.dataCenterId = FlowWriteConfig.DATA_CENTER_ID_NUM;
    }

    // ==============================Methods==========================================

    /**
     * 获得下一个ID (该方法是线程安全的)
     *
     * @return SnowflakeId
     */
    private synchronized long nextId() {
        long timestamp = timeGen();

        //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        if (timestamp < lastTimestamp) {
            throw new RuntimeException(
                    String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        //如果是同一时间生成的，则进行毫秒内序列
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            //毫秒内序列溢出
            if (sequence == 0) {
                //阻塞到下一个毫秒,获得新的时间戳
                timestamp = tilNextMillis(lastTimestamp);
            }
        }
        //时间戳改变，毫秒内序列重置
        else {
            sequence = 0L;
        }

        //上次生成ID的时间截
        lastTimestamp = timestamp;

        //移位并通过或运算拼到一起组成64位的ID
        return ((timestamp - twepoch) << timestampLeftShift)
                | (dataCenterId << dataCenterIdShift)
                | (workerId << workerIdShift)
                | sequence;
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     *
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 返回以毫秒为单位的当前时间
     *
     * @return 当前时间(毫秒)
     */
    protected long timeGen() {
        return System.currentTimeMillis();
    }


    /**
     * 静态工具类
     *
     * @return
     */
    public static Long generateId() {
        return idWorker.nextId();
    }


}