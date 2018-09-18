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
public class ExchangeTopicTest extends TestBase {

    final String EXCHANGE_NAME = "A.EXCHANGE.TOPIC";
    final String QUEUE_NAME = "A.QUEUE";
    final String QUEUE_NAME_2 = "A.QUEUE.2";

    final String EXCHANGE_TYPE = "topic";

    @Test
    public void producer() throws IOException, TimeoutException {

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true);

        // 使用模糊匹配将消息发送到匹配的队列上，“A.#”表示所有以“A.”开头的队列
        String message = "[topic]这是一条测试消息";
        channel.basicPublish(EXCHANGE_NAME, "A.#", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

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
