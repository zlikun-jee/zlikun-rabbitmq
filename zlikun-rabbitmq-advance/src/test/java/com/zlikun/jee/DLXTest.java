package com.zlikun.jee;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DLX （Dead Letter Exchange），死信交换器（死信箱），当一个消息在队列中变成死信后，可以重新发送到另一个Exchange中，
 * 这个Exchange就是DLX，与之绑定的队列即为“死信列队”<br>
 * 消息变成死信一般由以下几种情况导致：<br>
 * 1. 消息被拒绝（Basic.Reject / Basic.Nack），并且设置 requeue 参数为 false<br>
 * 2. 消息过期<br>
 * 3. 队列达到最大长度<br>
 * 实际上DLX也是一个正常的Exchange，与一般的Exchange没有区别，可以在任何队列上被指定，实际上就是设置某个队列的属性<br>
 * <a href="http://www.rabbitmq.com/dlx.html">http://www.rabbitmq.com/dlx.html</a> <br>
 *
 * @author zlikun
 * @date 2018-09-21 10:10
 */
@Slf4j
public class DLXTest extends TestBase {

    String exchange = "D.EXCHANGE";
    String routingKey = "";
    String queue = "D.QUEUE";
    String dlxExchange = "DLX.EXCHANGE";
    String dlxQueue = "DLX.QUEUE";
    String dlxRoutingKey = "";

    @Test
    public void producer() throws IOException, TimeoutException {
        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        // 声明死信交换器和死信队列
        channel.exchangeDeclare(dlxExchange, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(dlxQueue, true, false, false, null);
        channel.queueBind(dlxQueue, dlxExchange, dlxRoutingKey);

        // 声明测试交换器和队列
        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT, true);
        Map<String, Object> args = new HashMap<>(4);
        // 配置队列消息5秒过期
        args.put("x-message-ttl", 5000);
        // 配置队列指定死信交换器和死信路由键
        args.put("x-dead-letter-exchange", dlxExchange);
        // 如果没有特殊指定死信路由键，则使用原队列路由键
        args.put("x-dead-letter-routing-key", dlxRoutingKey);
        channel.queueDeclare(queue, true, false, false, args);
        channel.queueBind(queue, exchange, routingKey);

        // 发送一条消息，5秒过期，过期后会发送到DLX中（发送后，等待5秒，到管理后台去查找，或者写一段消费逻辑，从死信队列中读取消息）
        String message = "Hello !";
        channel.basicPublish(exchange, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

        channel.close();
        connection.close();

    }

    @Test
    public void consumer() throws IOException, TimeoutException, InterruptedException {

        final Connection connection = createConnectionFactory().newConnection();
        final Channel channel = connection.createChannel();

        // 等待5秒，测试队列消息会过期，重新投递到DLX，所以从死信队列中可以取得消息，而测试队列得不到
        // TimeUnit.SECONDS.sleep(5L);

        // 消费测试队列消息
        channel.basicConsume(queue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                log.info("testing => exchange = {}, routingKey = {}, deliveryTag = {}", envelope.getExchange(), envelope.getRoutingKey(), envelope.getDeliveryTag());
                log.info("testing => [{}] receive message: {}", consumerTag, new String(body));

                // 如果消息未过期，这里拒绝掉后，也会投递到死信队列，区别在于实际测试队列消费时已经取得过消息
                channel.basicReject(envelope.getDeliveryTag(), false);
            }
        });

        // 消费死信队列消息
        channel.basicConsume(dlxQueue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                log.info("dlx => exchange = {}, routingKey = {}, deliveryTag = {}", envelope.getExchange(), envelope.getRoutingKey(), envelope.getDeliveryTag());
                log.info("dlx => [{}] receive message: {}", consumerTag, new String(body));

                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        TimeUnit.SECONDS.sleep(3L);

        channel.close();
        connection.close();

    }

}
