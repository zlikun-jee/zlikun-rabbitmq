package com.zlikun.jee;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author zlikun
 * @date 2018-09-21 9:41
 */
@Slf4j
public class TTLTest extends TestBase {

    String exchange = "C.EXCHANGE";
    String routingKey = "logs";
    String queue = "C.QUEUE";

    @Test
    public void producer() throws IOException, TimeoutException {

        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT);

        // 声明队列时，通过参数指定消息默认过期时间，默认不过期，这里指定为60,000毫秒
        Map<String, Object> args = new HashMap<>(4);
        // 如果TTL设置为0，表示除非此时可以将消息投递给消费者，否则立即丢弃该消息
        args.put("x-message-ttl", 60000);
        // 该设置是针对队列本身的设置，表示指定时间（单位：毫秒）内不使用队列（没有消费者，也没有GET主动获取消息）或不重新声明队列，队列会被删除
        // 值规则与上面相同，但不能设置为0，在本例中，如果队列30分钟内未被使用，队列会被删除，如果不设置则队列永远不会删除
        args.put("x-expires", 30 * 3600);
        channel.queueDeclare(queue, true, false, false, args);

        channel.queueBind(queue, exchange, routingKey);

        // 发送消息
        String message = "Hello !";
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                // 持久化消息
                .deliveryMode(2)
                // 消息过期时间，与队列默认消息过期时间比较，取小的那个，单位：毫秒
                // 消息存在超过指定时间后，如果是队列级配置，会直接从队列中删除，如果是单条消息级配置，则会在消费时判断
                // 实际也会删除，只是发生的时机不同，队列级的过期消息肯定在头部，而消息级的势必要扫描全队列，所以不如等到消费时再判断
                // 在本例中，如果消息超过5秒没有被消费，那么就会过期
                .expiration("5000")
                .build();
        channel.basicPublish(exchange, routingKey, props, message.getBytes());

        channel.close();
        connection.close();
    }

    @Test
    public void consumer() throws IOException, TimeoutException {

        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        // 发送后快速执行消费（不超过5秒）         => 正确收到消息
        // 发送后等待一段时间再执行消费（超过5秒） => 没有收到消息
        channel.basicConsume(queue, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                log.info("exchange = {}, routingKey = {}, deliveryTag = {}", envelope.getExchange(), envelope.getRoutingKey(), envelope.getDeliveryTag());
                log.info("[{}] receive message: {}", consumerTag, new String(body));
            }
        });

        channel.close();
        connection.close();

    }

}
