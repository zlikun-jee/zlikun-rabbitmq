package com.zlikun.jee;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author zlikun
 * @date 2018-09-20 9:46
 */
@Slf4j
public class PublishMandatoryArgTest extends TestBase {

    String exchange = "B.EXCHANGE";
    String routingKey = "logs";
    String queue = "B.QUEUE";

    /**
     * 没有测出预期效果，后续再跟进
     *
     * @throws IOException
     * @throws TimeoutException
     */
    @Test
    public void mandatory() throws IOException, TimeoutException {

        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        // 解除绑定
        channel.queueUnbind(queue, exchange, routingKey);
        // 删除测试exchange
        channel.exchangeDelete(exchange);
        // 删除测试queue
        channel.queueDelete(queue);

        // 重新定义exchange
        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT);
        // 重新定义queue
        channel.queueDeclare(queue, true, false, false, null);
        // 重新绑定
        channel.queueBind(queue, exchange, routingKey);

        // mandatory => 为true时，当交换器无法根据自身的类型和路由键找到一个符合条件的队列时，会调用Basic.Return命令将消息返回给生产者，
        //           => 为false时，则会丢弃消息
        // 指定一个错误的routingKey，消息无法正常路由到绑定队列中
        channel.basicPublish(exchange,
                "logs_trace",
                true,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "Hello RabbitMQ !".getBytes());

        // mandatory => 为true时，没被正确路由到合适队列的消息会通过ReturnListener监听器来处理
        channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
            log.info("exchange = {}, routingKey = {}, replyCode = {}, replyText = {}, message = {}",
                    exchange, routingKey, replyCode, replyText, new String(body));
        });

        channel.close();
        connection.close();

    }

    @Test
    @Ignore
    public void immediate() {


    }

}
