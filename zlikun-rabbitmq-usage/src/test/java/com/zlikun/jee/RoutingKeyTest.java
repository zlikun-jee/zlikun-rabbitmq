package com.zlikun.jee;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author zlikun
 * @date 2018-09-18 9:58
 */
public class RoutingKeyTest extends TestBase {

    final String EXCHANGE_NAME = "A.EXCHANGE";
    final String QUEUE_NAME = "A.QUEUE";
    final String ROUTING_KEY = "A.ROUTING_KEY";

    @Test
    public void test() throws IOException, TimeoutException {

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 定义一个Exchange
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
        // 定义一个Queue
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        // 绑定Queue到Exchange，这里routingKey与bindingKey被视为等同的
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        // 发送消息
        String message = "这是一条测试消息";
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

        channel.close();
        connection.close();

    }

}
