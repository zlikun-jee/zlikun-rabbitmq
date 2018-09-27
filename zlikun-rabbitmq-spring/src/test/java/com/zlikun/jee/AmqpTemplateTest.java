package com.zlikun.jee;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author zlikun
 * @date 2018-09-27 10:55
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
public class AmqpTemplateTest {

    @Autowired
    private AmqpTemplate amqpTemplate;

    @Test
    public void test() {

        // 发送消息
        amqpTemplate.convertAndSend("spring.exchange.direct",
                "", "Hello RabbitMQ-1 !",
                message -> {
                    // MessagePostProcessor: Hello RabbitMQ!
                    log.info("MessagePostProcessor: {}", new String(message.getBody()));
                    return message;
                });

        // 发送消息
        amqpTemplate.send("spring.exchange.direct", "",
                new Message("Hello RabbitMQ-2 !".getBytes(),
                        MessagePropertiesBuilder.newInstance().build()));

    }

}
