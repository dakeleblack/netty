package com.bitao.task.netty;

import com.bitao.task.enums.MsgTypeEnum;
import com.bitao.task.msg.*;
import com.bitao.task.util.TaskCondition;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class NettyServerHandler extends SimpleChannelInboundHandler<BaseMsg> {
    private static final Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);
    // 维护任务,taskCondition是共享的
    private final TaskCondition taskCondition;

    private final NettyServer nettyServer;

    public NettyServerHandler(TaskCondition taskCondition, NettyServer nettyServer) {
        this.taskCondition = taskCondition;
        this.nettyServer = nettyServer;
    }

    /**
     * 服务端与客户端连接成功时服务端要做的事，这里发送一个LOGIN类型消息给客户端
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        logger.info("-------Netty服务端与Netty客户端连接成功");
        //生成这个nettyClient和nettyServer的唯一消息id
        String msgId = UUID.randomUUID().toString().replace("-", "");
        LoginMsg loginMsg = new LoginMsg();
        loginMsg.setMsgId(msgId);
        //发送消息到channel,即发送消息给客户端
        ctx.channel().writeAndFlush(loginMsg);
    }

    /**
     * 客户端与服务端断开连接时服务端要做的事
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.info("-------Netty服务端与Netty客户端断开连接");
        nettyServer.removeChannel((SocketChannel) ctx.channel());
    }

    /**
     * 服务端读取完成客户端发送过来的一条消息时，服务端做的事情
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        logger.info("-------Netty服务端接收Netty客户端一条数据完毕");
    }

    /**
     * 服务端收到客户端消息的处理方式，这里将收到的消息放入TaskCondition的taskQueue阻塞队列中
     *
     * @param ctx
     * @param msg 序列化后的消息
     * @throws Exception
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, BaseMsg msg) throws Exception {
        MsgTypeEnum msgTypeEnum = msg.getType();
        switch (msgTypeEnum) {
            case PING:
                logger.info("receive ping from nettyClient");
                ctx.channel().writeAndFlush(new PingMsg());
                break;
            case LOGIN:
                logger.info("receive login from nettyClient");
                break;
            case ASK:
                AskMsg askMsg = (AskMsg) msg;
                logger.info("receive ask from nettyClient");
                //将该客户端与服务端的socketChannel添加至NettyServer维护的映射中，key为消息id
                //对于某个客户端和服务端，其消息id一旦生成后是不会改变的
                nettyServer.addClientChannel(askMsg.getMsgId(), (SocketChannel) ctx.channel());
                //将任务参数添加到taskCondition的taskQueue阻塞队列中
                long taskInstanceId = taskCondition.addTask(askMsg.getTaskInstance(), askMsg.getMsgId());
                //将任务id添加到NettyServer维护的映射中，key为消息id
                nettyServer.addClientTaskInstanceId(askMsg.getMsgId(), taskInstanceId);
                //往client发送一个QUIT类型消息，表明server端已经接收到消息，可以关闭client了
                QuitMsg quitMsg = new QuitMsg();
                ctx.channel().writeAndFlush(quitMsg);
                break;
            default:
                break;
        }
        ReferenceCountUtil.release(msg);
    }

}
