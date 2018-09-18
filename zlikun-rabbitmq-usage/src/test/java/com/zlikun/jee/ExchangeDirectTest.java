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
public class ExchangeDirectTest extends TestBase {

    final String EXCHANGE_NAME = "A.EXCHANGE.DIRECT";
    final String QUEUE_NAME = "A.QUEUE";
    final String ROUTING_KEY = "A.ROUTING_KEY";

    final String EXCHANGE_TYPE = "direct";

    @Test
    public void producer() throws IOException, TimeoutException {

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true, false, null);
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        String message = "这是一条测试消息";
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

        for (int i = 0; i < 5; i++) {
            channel.basicConsume(QUEUE_NAME, consumer);
            TimeUnit.SECONDS.sleep(1L);
        }
        channel.close();
        connection.close();


    }

}
