package com.zlikun.jee;

import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author zlikun
 * @date 2018-09-20 10:27
 */
public class AlternateExchangeTest extends TestBase {

    @Test
    public void test() throws IOException, TimeoutException {

        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        Map<String, Object> args = new HashMap<>(4);
        args.put("alternate-exchange", "logs.exchange.backup");

        // 声明一个exchange，指定它的备份exchange
        channel.exchangeDeclare("logs.exchange", BuiltinExchangeType.DIRECT, true, false, args);
        // 声明一个exchange，其将被用于备份exchange，注意其类型为fanout类型
        channel.exchangeDeclare("logs.exchange.backup", BuiltinExchangeType.FANOUT);
        // 分别声明queue并绑定
        channel.queueDeclare("logs.queue", true, false, false, null);
        channel.queueBind("logs.queue", "logs.exchange", "logs");
        channel.queueDeclare("logs.queue.backup", true, false, false, null);
        channel.queueBind("logs.queue.backup", "logs.exchange.backup", "");

        // 由于使用的routingKey不是绑定时使用的routingKey，所以无法被正确路由
        // 此时消息将被路由到备份交换器上，进而发送到其绑定的队列上
        // 如果存在备份交换器的情况下，mandatory参数将无效
        channel.basicPublish("logs.exchange",
                "logs.error",
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "测试备份交换器".getBytes());

        channel.close();
        connection.close();

    }

}
