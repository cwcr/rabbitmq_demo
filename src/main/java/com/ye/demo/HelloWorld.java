package com.ye.demo;

import com.rabbitmq.client.*;
import com.sun.corba.se.impl.orbutil.threadpool.ThreadPoolImpl;
import com.ye.utils.ChannelUtil;
import sun.nio.ch.ThreadPool;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class HelloWorld {
    private static final String EXCHANGE_NAME = "exchange";
    private static final String QUEUE_NAME = "queue";
    private static final String ROUTING_NAME = "exchangeAndQueue";
    private static final Charset UTF_8 = Charset.forName("UTF8");
    public static void main(String[] args){
        try{
//            new MyReceiptThread().start();
            new MyReviewThread().start();
            return;
        }catch (Exception e){

        }
    }

    private static void receiptMessage() throws Exception{
        Channel channel = ChannelUtil.getChannel().get();
        //创建一个类型为 DIRECT 的数据交换器
        //true:持久化
        //false:非自动删除
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT,true,false,null);
        // 创建一个 name = QUEUE_NAME 消息队列
        channel.queueDeclare(QUEUE_NAME,true,false,false,null);
        // 注意绑定顺序，队列Name,交换器Name,指定路由Name
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,ROUTING_NAME);
        String message = "hello world";
        channel.basicPublish(EXCHANGE_NAME,ROUTING_NAME, MessageProperties.TEXT_PLAIN,message.getBytes(UTF_8));
        try{
            channel.close();
        }catch (Exception e){

        }
    }

    private static void reviewMessage(String s) throws Exception{
        Channel reviewChannel = ChannelUtil.getChannel().get();
        //设置客户端最多接收未被ack的消息的个数
        reviewChannel.basicQos(64);
        System.err.println(s);
        Consumer consumer = new DefaultConsumer(reviewChannel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.err.println("recv message:"+ new String(body));
                try{
                    TimeUnit.SECONDS.sleep(1);
                }catch (InterruptedException e){

                }
                reviewChannel.basicAck(envelope.getDeliveryTag(),false);
            }
        };
        reviewChannel.basicConsume(QUEUE_NAME,consumer);
        TimeUnit.SECONDS.sleep(1);
    }

    public static class MyReceiptThread extends Thread{
        @Override
        public void run() {
            while (true){
                try{
                    receiptMessage();
                }catch (Exception e){

                }
            }
        }
    }

    public static class MyReviewThread extends Thread{
        @Override
        public void run() {
            while (true){
                try{
                    reviewMessage("1");
                }catch (Exception e){

                }
            }
        }
    }
}
