package com.zlikun.jee.rpc;

import com.rabbitmq.client.*;
import com.zlikun.jee.TestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * http://www.rabbitmq.com/tutorials/tutorial-six-java.html <br>
 * https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/java/RPCServer.java <br>
 *
 * @author zlikun
 * @date 2018-09-25 10:57
 */
public class RPCServerTest extends TestBase {


    private Connection connection;
    private final String RPC_QUEUE_NAME = "RPC_QUEUE";

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

    private int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    @Test
    public void server() throws IOException {

        final Channel channel = connection.createChannel();

        // 定义RPC队列
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        // 清除队列中的数据
        channel.queuePurge(RPC_QUEUE_NAME);

        channel.basicQos(1);

        System.out.println("[server] Awaiting RPC requests");

        // 服务端消费RPC队列，对收到的消息（整数）做fib()处理，消息由客户端发送（RPC的调用）
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(properties.getCorrelationId())
                        .build();

                int result = 0;

                try {
                    int n = Integer.parseInt(new String(body));
                    result = fib(n);
                    System.out.printf("[server] fib(%d) => %s%n", n, result);
                } catch (RuntimeException e) {
                    System.out.println("[server] error => " + e.toString());
                } finally {
                    // 将计算结果发送到回调队列中（相当于返回给客户端，RPC的响应）
                    channel.basicPublish("", properties.getReplyTo(), replyProps, String.valueOf(result).getBytes());
                    // 发送完成后再作应答，表示一个RPC调用过程完成
                    channel.basicAck(envelope.getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (this) {
                        this.notify();
                    }
                }
            }
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

        // Wait and be prepared to consume the message from RPC client.
        while (true) {
            synchronized (consumer) {
                try {
                    consumer.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
