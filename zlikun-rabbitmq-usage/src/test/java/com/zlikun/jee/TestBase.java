package com.zlikun.jee;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.Before;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-05 10:39
 */
public abstract class TestBase {

    protected final String HOST = "rabbitmq.zlikun.com";

    protected ConnectionFactory factory;

    @Before
    public void init() {
        factory = new ConnectionFactory();
        // factory.setUri("amqp://root:123456@rabbitmq.zlikun.com:5672/");
        factory.setHost(HOST);
        factory.setUsername("root");
        factory.setPassword("123456");
        // factory.setVirtualHost("/");

        // 设置自动重连
        factory.setAutomaticRecoveryEnabled(true);
        // 设置每10秒，重试一次
        factory.setNetworkRecoveryInterval(10_000);
        // 设置重新声明交换器，队列等信息
        factory.setTopologyRecoveryEnabled(true);

        // 连接超时时间，单位：毫秒，默认：60秒
        factory.setConnectionTimeout(15_000);
        // 通道RPC调用超时时间，单位：毫秒，默认：10分钟
        factory.setChannelRpcTimeout(120_000);

    }

}
