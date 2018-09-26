package com.zlikun.jee;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * publisher confirm
 *
 * @author zlikun
 * @date 2018-09-26 9:50
 */
public class PublisherConfirmTest extends TestBase {

    private String QUEUE = "G.QUEUE";

    @Test
    public void sync() throws IOException, TimeoutException {

        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE, true, false, false, null);

        String message = "Hello RabbitMQ !";

        try {
            channel.confirmSelect();
            channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            if (!channel.waitForConfirms()) {
                System.out.println("Send message failed .");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        channel.close();
        connection.close();

    }

    @Test
    public void async() throws IOException, TimeoutException, InterruptedException {

        Connection connection = createConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE, true, false, false, null);

        String message = "Hello RabbitMQ !";

        SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<>());

        try {
            channel.confirmSelect();
            // 使用Listener实现确认（否认），该方式是异步方式
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    System.out.printf("Ack, SeqNo: %d, multiple: %s%n", deliveryTag, multiple);
                    if (multiple) {
                        confirmSet.headSet(deliveryTag - 1).clear();
                    } else {
                        confirmSet.remove(deliveryTag);
                    }
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    if (multiple) {
                        confirmSet.headSet(deliveryTag - 1).clear();
                    } else {
                        confirmSet.remove(deliveryTag);
                    }
                    // 消息重发
                }
            });

            while (true) {
                long nextSeqNo = channel.getNextPublishSeqNo();
                channel.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                confirmSet.add(nextSeqNo);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        channel.close();
        connection.close();

    }

}
