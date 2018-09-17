package com.zlikun.jee;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <a href="http://www.rabbitmq.com/tutorials/tutorial-one-java.html">
 * http://www.rabbitmq.com/tutorials/tutorial-one-java.html
 * </a>
 *
 * @author zlikun
 * @date 2018-09-17 10:41
 */
@Slf4j
public class AmqpTest extends TestBase {

    protected final String QUEUE_NAME = "A.TEST";

    @Test
    public void producer() throws Exception {

        // 创建一个连接
        Connection connection = factory.newConnection();
        // 创建一个信道
        Channel channel = connection.createChannel();

        // 声明交换器，默认：direct 交换器
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(QUEUE_NAME,
                false, false, false, null);
        log.info("QUEUE NAME is {}", declareOk.getQueue());

        // 创建纯文本消息
        // 发布消息
        String message = "Hello World!";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        log.info("[x] Sent '{}'", message);

        channel.close();
        connection.close();

    }

    @Test
    public void consumer() throws IOException, TimeoutException, InterruptedException {

        // 创建连接
        final Connection connection = factory.newConnection();
        // 创建信道
        final Channel channel = connection.createChannel();
        // 设置客户端最多接收未被ack的消息个数
        channel.basicQos(64);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                log.info("[{}] receive message: {}", consumerTag, new String(body));
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(QUEUE_NAME, consumer);
        TimeUnit.SECONDS.sleep(5L);
        channel.close();
        connection.close();

    }

}
