package com.chl.sentinel.database;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.datasource.AbstractDataSource;
import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.csp.sentinel.log.RecordLog;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

/**
 * @author ChenHailong
 * @date 2021/2/26 14:48
 **/
public class ZookeeperDataSource<T> extends AbstractDataSource<String, T> {

    public static final ExecutorService pool = new ThreadPoolExecutor(8, 16, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(100), new NamedThreadFactory("sentinel-zookeeper-ds-update"),
            new ThreadPoolExecutor.DiscardOldestPolicy());

    private final String path;

    private NodeCacheListener listener;

    private CuratorFramework zkClient;

    private NodeCache nodeCache = null;

    public ZookeeperDataSource(final CuratorFramework zkClient, final String path, Converter<String, T> parser) {
        super(parser);

        this.zkClient = zkClient;
        this.path = path;

        String serverAddr = null;
        init(serverAddr, null);
    }

    private void init(final String serverAddr, final List<AuthInfo> authInfos) {
        initZookeeperListener(serverAddr, authInfos);
        loadInitialConfig();
    }

    private void loadInitialConfig() {
        try {
            T newValue = loadConfig();
            if (newValue == null) {
                RecordLog
                        .warn("[ZookeeperDataSource] WARN: initial config is null, you may have to check your data source");
            }
            getProperty().updateValue(newValue);
        } catch (Exception ex) {
            RecordLog.warn("[ZookeeperDataSource] Error when loading initial config", ex);
        }
    }

    private void initZookeeperListener(final String serverAddr, final List<AuthInfo> authInfos) {
        try {
            this.listener = new NodeCacheListener() {

                @Override
                public void nodeChanged() {

                    try {
                        T newValue = loadConfig();
                        RecordLog.info(String
                                .format("[ZookeeperDataSource] New property value received for (%s, %s): %s",
                                        serverAddr, path, newValue));
                        // Update the new value to the property.
                        getProperty().updateValue(newValue);
                    } catch (Exception ex) {
                        RecordLog.warn("[ZookeeperDataSource] loadConfig exception", ex);
                    }
                }
            };

            this.nodeCache = new NodeCache(this.zkClient, this.path);
            this.nodeCache.getListenable().addListener(this.listener, ZookeeperDataSource.pool);
            this.nodeCache.start(true);
        } catch (Exception e) {
            RecordLog.warn("[ZookeeperDataSource] Error occurred when initializing Zookeeper data source", e);
            e.printStackTrace();
        }
    }

    @Override
    public String readSource() throws Exception {
        if (this.zkClient == null) {
            throw new IllegalStateException("Zookeeper has not been initialized or error occurred");
        }
        String configInfo = null;
        ChildData childData = nodeCache.getCurrentData();
        if (null != childData && childData.getData() != null) {

            configInfo = new String(childData.getData());
        }
        return configInfo;
    }

    @Override
    public void close() throws Exception {
        if (this.nodeCache != null) {
            this.nodeCache.getListenable().removeListener(listener);
            this.nodeCache.close();
        }
        if (this.zkClient != null) {
            this.zkClient.close();
        }
        ZookeeperDataSource.pool.shutdown();
    }
}
