package com.zlikun.jee.api;

import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * http://www.rabbitmq.com/api-guide.html#connecting
 * @author zlikun
 * @date 2018-09-29 10:13
 */
public class ConnectingTest {

    final String HOST = "rabbitmq.zlikun.com";
    final int PORT = 5672;
    final String USERNAME = "root";
    final String PASSWORD = "123456";
    final String VIRTUAL_HOST = "/";

    /**
     * 配置方法一
     */
    @Test
    public void one() throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setVirtualHost(VIRTUAL_HOST);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);

        // 测试连接
        connecting(factory);

    }

    /**
     * 配置方法二
     *
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @throws URISyntaxException
     */
    @Test
    public void two() throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(String.format("amqp://%s:%s@%s:%d/%s", USERNAME, PASSWORD, HOST, PORT, VIRTUAL_HOST));

        // 测试连接
        connecting(factory);

    }

    private void connecting(ConnectionFactory factory) throws IOException, TimeoutException {
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare();
        channel.basicPublish("",
                declareOk.getQueue(),
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "Hello RabbitMQ !".getBytes());

        channel.close();
        connection.close();
    }

}
