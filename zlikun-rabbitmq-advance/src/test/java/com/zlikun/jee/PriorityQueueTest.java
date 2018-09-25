package com.zlikun.jee;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 优先级队列<br>
 * 当消费者速度比生产者慢时才有意义（否则队列中最多只有一条记录，优先级失去意义）<br>
 * 优先级取值范围[0, 10]，0最低、10最高
 *
 * @author zlikun
 * @date 2018-09-25 10:00
 */
@Slf4j
public class PriorityQueueTest extends TestBase {

    String exchange = "E.EXCHANGE";
    String routingKey = "";
    String queue = "E.QUEUE";

    @Test
    public void producer() throws IOException, TimeoutException {
        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
        // 创建队列时，指定最大优先级
        Map<String, Object> args = new HashMap<>(4);
        args.put("x-max-priority", 10);
        channel.queueDeclare(queue, true, false, false, args);
        channel.queueBind(queue, exchange, routingKey);

        // 发送消息时，指定优先级
        String message = "Hello RabbitMQ!";
        channel.basicPublish(exchange, routingKey, new AMQP.BasicProperties.Builder()
                        .priority(5)
                        .build(),
                message.getBytes());

        channel.close();
        connection.close();

    }

}
