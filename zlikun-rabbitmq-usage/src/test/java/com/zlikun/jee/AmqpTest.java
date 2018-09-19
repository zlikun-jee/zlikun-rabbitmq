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

                log.info("exchange = {}, routingKey = {}, deliveryTag = {}", envelope.getExchange(), envelope.getRoutingKey(), envelope.getDeliveryTag());

                log.info("[{}] receive message: {}", consumerTag, new String(body));
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 显示进行Ack（应答）操作
                channel.basicAck(envelope.getDeliveryTag(), false);

                // 显示拒绝消息：requeue=true，则消息重新入队，等待下一次消费，requeue=false，则直接删除消息（会启用死信队列功能）
                // basicReject() 一次只能拒绝一条消息，如果要批量拒绝，使用basicNack() 来实现
                // channel.basicReject(envelope.getDeliveryTag(), false);

                // multiple=false，表示拒绝当前消息(deliveryTag)，multiple=true，表示拒绝当前消息之前所有未确认的消息
                // channel.basicNack(envelope.getDeliveryTag(), true, true);
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                log.info("closed channel or connection, consumerTag = {}, reason = {}", consumerTag, sig.getReason());
            }
        };

        // autoAck = false，关闭自动应答
        channel.basicConsume(QUEUE_NAME, false, consumer);
        TimeUnit.SECONDS.sleep(5L);

        // 建议显示关闭channel，但如果不关闭在connection关闭时，channel也会关闭
        channel.close();
        connection.close();

    }

}
