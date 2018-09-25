package com.zlikun.jee.rpc;

import com.rabbitmq.client.*;
import com.zlikun.jee.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * http://www.rabbitmq.com/tutorials/tutorial-six-java.html <br>
 * https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/java/RPCClient.java <br>
 *
 * @author zlikun
 * @date 2018-09-25 10:59
 */
public class RPCClientTest extends TestBase {

    private Connection connection;
    private String RPC_QUEUE_NAME = "RPC_QUEUE";

    @Before
    public void init() throws IOException, TimeoutException {
        connection = createConnectionFactory().newConnection();
    }

    @After
    public void destroy() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void client() throws IOException, TimeoutException, InterruptedException {

        Channel channel = connection.createChannel();
        for (int i = 0; i < 32; i++) {
            String message = Integer.toString(i + 1);
            System.out.printf(" [client] Requesting fib(%s)%n", message);
            System.out.printf(" [client] Got '%s'%n", call(channel, message));
        }
        channel.close();

    }

    private String call(Channel channel, String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        // 使用系统生成的队列作为回调队列，客户端监听消费该队列（接收RPC服务端的响应信息）
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        // 向服务端发送RPC请求（向RPC队列推送消息）
        channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));

        final BlockingQueue<String> responseQueue = new ArrayBlockingQueue<>(1);

        String consumerTag = channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if (properties.getCorrelationId().equals(corrId)) {
                    responseQueue.offer(new String(body));
                }
            }
        });

        String result = responseQueue.take();
        channel.basicCancel(consumerTag);
        return result;
    }


}
