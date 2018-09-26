package com.zlikun.jee;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 事务机制
 *
 * @author zlikun
 * @date 2018-09-26 9:37
 */
public class TransactionTest extends TestBase {

    private String QUEUE = "F.QUEUE";

    @Test
    public void test() throws IOException, TimeoutException {

        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE, true, false, false, null);

        String message = "Hello RabbitMQ !";

        try {
            // 开启事务
            channel.txSelect();
            channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
//            // 抛出一个异常，测试事务回滚
//            System.out.println(1 / 0);
            // 提交事务
            channel.txCommit();
        } catch (Exception e) {
            // 回滚事务
            channel.txRollback();
        }

        channel.close();
        connection.close();

    }

}
