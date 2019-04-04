package com.ye.utils;

import com.google.common.collect.Lists;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ChannelUtil {
    private static final String IP_ADDRESS = "47.106.118.153";
    private static final int PORT = 5672;
    private static ConnectionFactory connectionFactory;
    private static final int MAX_CONNECT_SIZE = 5;
    private static Queue<Connection> connections;

    static{
        //创建连接工厂并提供参数
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(IP_ADDRESS);
        connectionFactory.setPort(PORT);
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connections = new LinkedBlockingQueue<>();
        for (int i = 0; i < MAX_CONNECT_SIZE; i++) {
            try{
                connections.add(connectionFactory.newConnection());
            }catch (Exception e){

            }
        }
    }

    public static Optional<Channel> getChannel() throws IOException,InterruptedException{
        while (true){
            Connection pop = connections.poll();
            Channel result;
            if(pop != null){
                result = pop.createChannel();
                connections.add(pop);
                return Optional.of(result);
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
