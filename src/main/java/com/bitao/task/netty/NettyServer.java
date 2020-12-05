package com.bitao.task.netty;

import com.bitao.task.util.TaskCondition;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NettyServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private int port;

    //共享的TaskCondition对象
    private TaskCondition taskCondition;

    //两个线程池
    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    //存放server端维护的客户端channel集合，key为消息id，value为该消息id对应的channel，一个客户端与服务端的所有消息只用一个clientId
    private Map<String, SocketChannel> clientId2Channel = new ConcurrentHashMap<>();

    //存放server端维护的任务id集合，key为消息id，value为该消息id对应的任务id
    private Map<String, Long> clientId2TaskInstanceId = new ConcurrentHashMap<>();

    public NettyServer(int port, TaskCondition taskCondition) {
        this.port = port;
        this.taskCondition = taskCondition;
    }

    public void addClientChannel(String clientId, SocketChannel socketChannel) {
        clientId2Channel.putIfAbsent(clientId, socketChannel);
    }

    public void addClientTaskInstanceId(String clientId, long taskInstanceId) {
        clientId2TaskInstanceId.putIfAbsent(clientId, taskInstanceId);
    }

    public void removeChannel(String clientId, SocketChannel socketChannel) {
        clientId2Channel.remove(clientId, socketChannel);
    }

    public void removeChannel(SocketChannel socketChannel) {
        clientId2Channel.entrySet().removeIf(entry -> entry.getValue() == socketChannel);
    }

    public void removeChannel(String clientId) {
        clientId2Channel.remove(clientId);
    }

    public Channel getChannelByClientId(String clientId) {
        return clientId2Channel.get(clientId);
    }

    public long getTaskInstanceIdByClientId(String clientId) {
        return clientId2TaskInstanceId.get(clientId);
    }

    public void bind() {
        //先实例化两个线程池
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap sb = new ServerBootstrap();
        //获取当前的NettyServer对象，传给NettyServerHandler，用于操作client2Channel以及client2TaskInstanceId集合
        NettyServer nettyServer = this;
        //netty的一些必要配置
        sb.group(bossGroup, workerGroup).
                channel(NioServerSocketChannel.class).
                option(ChannelOption.SO_BACKLOG, 1024).
                option(ChannelOption.SO_SNDBUF, 32 * 1204).
                option(ChannelOption.SO_RCVBUF, 32 * 1204).
                option(ChannelOption.SO_KEEPALIVE, true).
                childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        //添加消息的编码和解码方式，若发送的消息为ByteBuf类型，则不需要设置编码和解码类,若发送的消息为String类型
                        //则对应的编码和解码类为StringEncoder和StringDecoder，若是自定义的消息（比如是个对象），则设置编解码
                        //类为ObjectEncoder和ObjectDecoder
                        pipeline.addLast(new ObjectEncoder());
                        pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                        //添加NettyClient的消息处理类NettyServerHandler
                        pipeline.addLast(new NettyServerHandler(taskCondition, nettyServer));
                    }
                });
        ChannelFuture channelFuture;
        try {
            channelFuture = sb.bind(port).sync();//绑定特定端口，启动服务端
            if (channelFuture.isSuccess()) {
                logger.info("-------Netty服务端启动成功");
            } else {
                logger.error("-------Netty服务端启动失败");
            }
            //TODO 等待服务端关闭，否则阻塞于此，这个代码要注释掉，否则主线程（MasterServer.run()）会一直阻塞于此，无法执行后续的线程池操作
//            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭NettyServer
     * bossGroup和workGroup是两个非守护线程，需要显式关闭，NettyServer才能关闭
     */
    public void closeNettyServer() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
