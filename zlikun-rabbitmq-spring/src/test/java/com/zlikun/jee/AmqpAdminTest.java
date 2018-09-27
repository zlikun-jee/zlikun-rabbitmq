package com.zlikun.jee;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

/**
 * @author zlikun
 * @date 2018-09-27 10:54
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class AmqpAdminTest {

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Test
    public void test() {

        // 定义Exchange，Spring对Exchange类型做了语义化（各有一个Exchange类来对应）
        DirectExchange exchange = new DirectExchange("spring.exchange.direct",
                true, false, null);
        amqpAdmin.declareExchange(exchange);

        // 定义Queue
        Queue queue = new Queue("spring.queue.direct",
                true, false, false, null);
        String queueName = amqpAdmin.declareQueue(queue);
        assertEquals("spring.queue.direct", queueName);

        // 通过routingKey绑定queue到exchange
        amqpAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(""));

    }

}
