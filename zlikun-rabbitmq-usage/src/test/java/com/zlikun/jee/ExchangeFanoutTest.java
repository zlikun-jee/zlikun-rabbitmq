package com.zlikun.jee;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author zlikun
 * @date 2018-09-18 10:54
 */
@Slf4j
public class ExchangeFanoutTest extends TestBase {

    final String EXCHANGE_NAME = "A.EXCHANGE.FANOUT";
    final String QUEUE_NAME = "A.QUEUE";
    final String QUEUE_NAME_2 = "A.QUEUE.2";
    final String ROUTING_KEY = "A.ROUTING_KEY";

    final String EXCHANGE_TYPE = "fanout";

    @Test
    public void producer() throws IOException, TimeoutException {

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false, null);
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueDeclare(QUEUE_NAME_2, true, false, false, null);
        // 绑定多个队列
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        channel.queueBind(QUEUE_NAME_2, EXCHANGE_NAME, ROUTING_KEY);

        // 这条消息将被发送到所有绑定到 A.EXCHANGE.FANOUT 交换机的队列上
        String message = "[fanout]这是一条测试消息";
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

        channel.close();
        connection.close();

    }

    @Test
    public void consumer() throws IOException, TimeoutException, InterruptedException {

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

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
        channel.basicConsume(QUEUE_NAME_2, consumer);

        TimeUnit.SECONDS.sleep(5L);

        channel.close();
        connection.close();

    }

}
