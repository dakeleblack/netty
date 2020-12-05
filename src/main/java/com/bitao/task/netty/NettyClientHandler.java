package com.bitao.task.netty;

import com.bitao.task.enums.MsgTypeEnum;
import com.bitao.task.msg.AskMsg;
import com.bitao.task.msg.BaseMsg;
import com.bitao.task.msg.LoginMsg;
import com.bitao.task.util.TaskInstance;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyClientHandler extends SimpleChannelInboundHandler<BaseMsg> {

    private static final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);

    private TaskInstance taskInstance;

    public NettyClientHandler(TaskInstance taskInstance) {
        this.taskInstance = taskInstance;
    }

    /**
     * 客户端与服务端连接成功时，客户端做的事情
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        LoginMsg loginMsg = new LoginMsg();
        ctx.channel().writeAndFlush(loginMsg);
        logger.info("-------Netty客户端与Netty服务端连接成功");
    }

    /**
     * 客户端与服务端断开连接时，客户端做的事情
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.info("--------Netty客户端与Netty服务端断开连接");
    }

    /**
     * 客户端读取完成服务端发送过来的一条消息时，客户端做的事情
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        logger.info("--------Netty客户端接收Netty服务端的一条消息完毕");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, BaseMsg msg) throws Exception {
        MsgTypeEnum msgTypeEnum = msg.getType();
        switch (msgTypeEnum) {
            case PING:
                logger.info("receive ping from nettyServer");
                break;
            case LOGIN:
                logger.info("receive login from nettyServer");
                AskMsg askMsg = new AskMsg();
                askMsg.setTaskInstance(taskInstance);
                askMsg.setMsgId(msg.getMsgId());
                ctx.channel().writeAndFlush(askMsg);
                break;
            case ASK:
                logger.info("receive ask from nettyServer");
                break;
            case QUIT:
                logger.info("receive quit from nettyServer");
                //接收到server发送过来的QUIT消息后，client就可以关闭对应的channel了，此时会执行clientHandler的channelInactive()
                //就可以通过ChannelHandler.channel().close()关闭这个client对应的channel，但这里不采用这种方式，而是采用当任务执行
                //结束后，activeThread集合中对应该任务的线程执行结束，然后再获取该线程对象中的taskInstance对象中的clientId得到
                //对应的SocketChannel，然后关闭此channel的方式来关闭对应的client
//                ctx.channel().close();
                break;
            default:
                break;
        }
        ReferenceCountUtil.release(msg);
    }
}
