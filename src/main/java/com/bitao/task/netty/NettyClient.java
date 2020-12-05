package com.bitao.task.netty;

import com.bitao.task.util.TaskInstance;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 当客户端channel主动关闭连接时（即客户端调用SocketChannel.close()或socketChannel.closeFuture()），客户端会向服务端发送一个写请求，
 * 然后服务端channel所在线程上的selector会监听到一个OP_READ事件，然后服务端执行数据读取操作，但服务端在读取数据时发现对应客户端channel
 * 已经关闭了，则读取数据字节个数返回-1，然后执行服务端channel的close操作，关闭该channel对应的底层socket，并在channelPipline中，从
 * 第一个channelHandler开始，触发channelHandler的channelInactive和channelUnregistered方法的执行，然后移除channelPipline中的
 * channelHandler
 */
public class NettyClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private String host;

    private int port;

    private SocketChannel socketChannel;

    private TaskInstance taskInstance;

    public NettyClient(String host, int port, TaskInstance<String> taskInstance) {
        this.host = host;
        this.port = port;
        this.taskInstance = taskInstance;
    }

    public void start() {
        NioEventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group).
                channel(NioSocketChannel.class).
                option(ChannelOption.SO_KEEPALIVE, true).
                remoteAddress(host, port).
                handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        //添加消息的编码和解码方式，若发送的消息ByteBuf，则不需要设置编码和解码类
                        pipeline.addLast(new ObjectEncoder());
                        pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                        //添加自定义消息处理器
                        socketChannel.pipeline().addLast(new NettyClientHandler(taskInstance));
                    }
                });
        ChannelFuture future;
        try {
            future = bootstrap.connect(host, port).sync();
            if (future.isSuccess()) {
                //获取该netty客户端对应的Channel,一个Channel对应一个线程
                socketChannel = (SocketChannel) future.channel();
                logger.info("------Netty客户端连接成功");
            } else {
                logger.error("------Netty客户端连接失败");
            }
            //等待客户端关闭，否则阻塞于此
            socketChannel.closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            logger.info("-------channel closed");
            group.shutdownGracefully();
        }
    }
}
