package com.bitao.task.scheduler;

import com.bitao.task.netty.NettyServer;
import com.bitao.task.threadpool.MasterSingleThread;
import com.bitao.task.util.Stopper;
import com.bitao.task.util.TaskCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class MasterServer {
    private static final Logger logger = LoggerFactory.getLogger(MasterServer.class);

    private static final ThreadFactory threadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("master-single-thread-");
        return thread;
    };
    //该线程池所建线程为守护线程，在程序中所有非守护线程执行结束后，程序才会结束，而守护线程可以一直存在
    private ExecutorService masterSingleExecutorService = Executors.newSingleThreadExecutor(threadFactory);
    //共享的TaskCondition对象
    private TaskCondition taskCondition;
    //MasterServer负责启动Netty服务端，负责连接的连接与客户端消息的处理
    private NettyServer nettyServer;

    public MasterServer(TaskCondition taskCondition) {
        this.taskCondition = taskCondition;
    }

    public void run() {
        try {
            //根据传入的TaskCondition实例化一个NettyServer，绑定特定端口，启动NettyServer
            nettyServer = new NettyServer(8888, taskCondition);
            nettyServer.bind();
        } catch (Exception e) {
            logger.info("start netty server fail:" + e.getMessage());
            return;
        }
        masterSingleExecutorService.execute(new MasterSingleThread(nettyServer, taskCondition));
        logger.info("start master success");
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public void stop() {
        if (Stopper.isStopped()) {
            return;
        }
        Stopper.stop();
        //关闭线程池以及NettyServer
        try {
            masterSingleExecutorService.shutdown();
            masterSingleExecutorService.awaitTermination(1, TimeUnit.SECONDS);
            nettyServer.closeNettyServer();
        } catch (Exception e) {
            logger.error("close masterServer fail:", e);
        }
    }

}
