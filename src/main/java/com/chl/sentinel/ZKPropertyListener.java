package com.chl.sentinel;

import java.util.Collections;
import java.util.List;

import com.alibaba.csp.sentinel.property.PropertyListener;
import com.alibaba.fastjson.JSON;
import com.chl.sentinel.database.ZookeeperDataSource;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * zk database 数据更新的监听器
 * @author ChenHailong
 * @date 2021/3/1 12:57
 **/
@Setter
@Slf4j
public class ZKPropertyListener<T> implements PropertyListener<List<T>> {

    String path;

    CuratorFramework zkClient;

    private boolean isEqual(Object oldValue, Object newValue) {
        if (oldValue == null && newValue == null) {
            return true;
        }
        else {
            return oldValue == null ? false : oldValue.equals(newValue);
        }
    }

    @Override
    public void configUpdate(List<T> value) {
        try {
            initPath();
        } catch (Exception e) {
            ZKPropertyListener.log.error("initPath|{}", path, e);
            return;
        }

        if (null == value) {
            value = Collections.EMPTY_LIST;
        }

        String lockPath = getLockPath(value);

        try {
            gainLock(lockPath);
        } catch (Exception e) {
            return;
        }

        try {
            // 判断value是否改变：从zk获取的值 对比 参数值。避免重复修改。
            String text = new String(zkClient.getData().forPath(path));
            List list = JSON.parseObject(text, List.class);
            if (isEqual(list, value)) {
                return;
            }
            /*try {
                Field valueField;
                valueField = property.getClass().getDeclaredField("value");
                valueField.setAccessible(true);
                Object currentValue = valueField.get(property);
                if (isEqual(currentValue, value)) {
                    return;
                }
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }*/
        } catch (Exception e) {
        }

        try {
            // send to zk
            String data = JSON.toJSONString(value);
            zkClient.setData().forPath(path, data.getBytes());
        } catch (Exception e) {
            ZKPropertyListener.log.error("checkAndModifyPath|{}", path, e);
        } finally {
            releaseLock(lockPath);
        }
    }

    /**
     * 释放lock
     * @param lockPath
     */
    private void releaseLock(String lockPath) {
        ZookeeperDataSource.pool.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    Stat stat = zkClient.checkExists().forPath(lockPath);
                    if (null == stat) {
                        return;
                    }

                    Thread.sleep(3000);
                    zkClient.delete().forPath(lockPath);
                } catch (Exception e) {
                }
            }
        });
    }

    /**
     * 获取lock
     * 确保path数据只被一个app node负责更新
     * @param lockPath
     * @throws Exception
     */
    private void gainLock(String lockPath) throws Exception {
        // 确保path数据只被一个app node负责更新
        Stat stat = zkClient.checkExists().forPath(lockPath);
        // lockpath 已经存在，表示有其他app node负责持久化到zk
        if (null != stat) {
            return;
        }
        // 相同path的node已经存在，则抛出异常，表示有其他app node负责持久化到zk
        // org.apache.zookeeper.ZooKeeper.create
        zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(lockPath);
    }

    /**
     * lockpath
     * @param value
     * @return
     */
    private String getLockPath(List<T> value) {
        String lockPath = path + "/lockHashCode" + value.hashCode();
        return lockPath;
    }

    /**
     * 创建存储规则的path
     * @throws Exception
     */
    private void initPath() throws Exception {
        // 节点存在
        Stat stat = zkClient.checkExists().forPath(path);
        if (stat == null) {
            zkClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
        }
    }

    @Override
    public void configLoad(List<T> flowRules) {

    }

}
