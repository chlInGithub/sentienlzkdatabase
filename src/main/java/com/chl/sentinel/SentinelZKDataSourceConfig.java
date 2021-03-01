package com.chl.sentinel;

import java.util.List;

import com.alibaba.csp.sentinel.datasource.ReadableDataSource;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRule;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRuleManager;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRuleManager;
import com.alibaba.csp.sentinel.slots.system.SystemRule;
import com.alibaba.csp.sentinel.slots.system.SystemRuleManager;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.chl.sentinel.database.ZookeeperDataSource;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 利用springboot autoconfig生成 各种规则对应的zk database与listener。
 * @author ChenHailong
 * @date 2021/2/25 17:13
 **/
@Configuration
@ConfigurationProperties(prefix = "sentinel.datasource.zk")
@Setter
public class SentinelZKDataSourceConfig {

    // zk地址  ip:port
    String remoteAddress;

    // 流控
    String flowPath;

    // 降级
    String degradePath;

    // 热点
    String paramFlowPath;

    // 系统
    String systemPath;

    // 授权
    String authorityPath;

    @Bean
    @ConditionalOnProperty(name = "sentinel.datasource.zk.remoteAddress")
    public CuratorFramework zkClient() {
        if (StringUtils.isBlank(remoteAddress)) {
            throw new Error("remoteAddress is null");
        }

        final int RETRY_TIMES = 3;
        final int SLEEP_TIME = 1000;
        CuratorFramework zkClient = CuratorFrameworkFactory
                .newClient(remoteAddress, new ExponentialBackoffRetry(SLEEP_TIME, RETRY_TIMES));
        zkClient.start();

        return zkClient;
    }

    @Bean
    @ConditionalOnClass(FlowRuleManager.class)
    @ConditionalOnProperty(name = "sentinel.datasource.zk.flowPath")
    public ZookeeperDataSource zookeeperDataSource4Flow(CuratorFramework zkClient) {
        final String path = flowPath;
        if (StringUtils.isBlank(path)) {
            throw new Error("flowPath is null");
        }

        ZookeeperDataSource<List<FlowRule>> zookeeperDataSource = new ZookeeperDataSource<>(zkClient, path,
                source -> JSON.parseObject(source, new TypeReference<List<FlowRule>>() {

                }));

        ReadableDataSource<String, List<FlowRule>> flowRuleDataSource = zookeeperDataSource;
        FlowRuleManager.register2Property(flowRuleDataSource.getProperty());
        modifyFlowRuleManager(zkClient, zookeeperDataSource, path);

        return zookeeperDataSource;
    }

    @Bean
    @ConditionalOnClass(DegradeRuleManager.class)
    @ConditionalOnProperty(name = "sentinel.datasource.zk.degradePath")
    public ZookeeperDataSource zookeeperDataSource4Degrade(CuratorFramework zkClient) {
        final String path = degradePath;
        if (StringUtils.isBlank(path)) {
            throw new Error("degradePath is null");
        }

        ZookeeperDataSource<List<DegradeRule>> zookeeperDataSource = new ZookeeperDataSource<>(zkClient, path,
                source -> JSON.parseObject(source, new TypeReference<List<DegradeRule>>() {

                }));

        ReadableDataSource<String, List<DegradeRule>> flowRuleDataSource = zookeeperDataSource;
        DegradeRuleManager.register2Property(flowRuleDataSource.getProperty());
        modifyFlowRuleManager(zkClient, zookeeperDataSource, path);

        return zookeeperDataSource;
    }

    @Bean
    @ConditionalOnClass(ParamFlowRuleManager.class)
    @ConditionalOnProperty(name = "sentinel.datasource.zk.paramFlowPath")
    public ZookeeperDataSource zookeeperDataSource4ParamFlow(CuratorFramework zkClient) {
        final String path = paramFlowPath;
        if (StringUtils.isBlank(path)) {
            throw new Error("paramFlowPath is null");
        }

        ZookeeperDataSource<List<ParamFlowRule>> zookeeperDataSource = new ZookeeperDataSource<>(zkClient, path,
                source -> JSON.parseObject(source, new TypeReference<List<ParamFlowRule>>() {

                }));

        ReadableDataSource<String, List<ParamFlowRule>> flowRuleDataSource = zookeeperDataSource;
        ParamFlowRuleManager.register2Property(flowRuleDataSource.getProperty());
        modifyFlowRuleManager(zkClient, zookeeperDataSource, path);

        return zookeeperDataSource;
    }

    @Bean
    @ConditionalOnClass(ParamFlowRuleManager.class)
    @ConditionalOnProperty(name = "sentinel.datasource.zk.systemPath")
    public ZookeeperDataSource zookeeperDataSource4System(CuratorFramework zkClient) {
        final String path = systemPath;
        if (StringUtils.isBlank(path)) {
            throw new Error("systemPath is null");
        }

        ZookeeperDataSource<List<SystemRule>> zookeeperDataSource = new ZookeeperDataSource<>(zkClient, path,
                source -> JSON.parseObject(source, new TypeReference<List<SystemRule>>() {

                }));

        ReadableDataSource<String, List<SystemRule>> flowRuleDataSource = zookeeperDataSource;
        SystemRuleManager.register2Property(flowRuleDataSource.getProperty());
        modifyFlowRuleManager(zkClient, zookeeperDataSource, path);

        return zookeeperDataSource;
    }

    @Bean
    @ConditionalOnClass(ParamFlowRuleManager.class)
    @ConditionalOnProperty(name = "sentinel.datasource.zk.authorityPath")
    public ZookeeperDataSource zookeeperDataSource4Authority(CuratorFramework zkClient) {
        final String path = authorityPath;
        if (StringUtils.isBlank(path)) {
            throw new Error("authorityPath is null");
        }

        ZookeeperDataSource<List<AuthorityRule>> zookeeperDataSource = new ZookeeperDataSource<>(zkClient, path,
                source -> JSON.parseObject(source, new TypeReference<List<AuthorityRule>>() {

                }));

        ReadableDataSource<String, List<AuthorityRule>> flowRuleDataSource = zookeeperDataSource;
        AuthorityRuleManager.register2Property(flowRuleDataSource.getProperty());
        modifyFlowRuleManager(zkClient, zookeeperDataSource, path);

        return zookeeperDataSource;
    }

    private <T> void modifyFlowRuleManager(CuratorFramework zkClient, ZookeeperDataSource<List<T>> zookeeperDataSource,
            String path) {
        try {
            ZKPropertyListener flowPropertyListener = new ZKPropertyListener();
            flowPropertyListener.setZkClient(zkClient);
            flowPropertyListener.setPath(path);

            zookeeperDataSource.getProperty().addListener(flowPropertyListener);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
