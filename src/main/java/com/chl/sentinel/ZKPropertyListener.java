package com.chl.sentinel;

import java.util.List;

import com.alibaba.csp.sentinel.property.PropertyListener;
import com.alibaba.fastjson.JSON;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * zk database 数据更新的监听器
 * @author ChenHailong
 * @date 2021/3/1 12:57
 **/
@Setter
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
        // 节点存在
        try {
            Stat stat = zkClient.checkExists().forPath(path);
            if (stat == null) {
                zkClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                        .forPath(path, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 判断value是否改变：从zk获取的值 对比 参数值。避免重复修改。
        try {
            String text = new String(zkClient.getData().forPath(path));
            List list = JSON.parseObject(text, List.class);
            if (isEqual(list, value)) {
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
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

        // send to zk
        String data = JSON.toJSONString(value);
        try {
            zkClient.setData().forPath(path, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void configLoad(List<T> flowRules) {

    }

}
