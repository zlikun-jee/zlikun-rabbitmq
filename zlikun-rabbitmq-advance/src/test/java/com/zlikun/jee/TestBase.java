package com.zlikun.jee;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.Before;

/**
 * @author zlikun
 * @date 2018-09-20 09:39
 */
public abstract class TestBase {

    protected final String HOST = "rabbitmq.zlikun.com";

    /**
     * 创建一个连接工厂
     *
     * @return
     */
    protected ConnectionFactory createConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setUsername("root");
        factory.setPassword("123456");
        // factory.setVirtualHost("/");

        return factory;
    }

}
