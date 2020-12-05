package com.bitao.task.app;

import com.bitao.task.enums.TaskState;
import com.bitao.task.netty.NettyClient;
import com.bitao.task.util.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * NettyClient端，负责任务消息的发送
 */
public class SchedulerClientStart {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerClientStart.class);

    private static final ThreadPoolExecutor pool = new ThreadPoolExecutor(
            10,
            20,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(16));

    public static void main(String[] args) {
        IntStream.range(0, 5).forEach(i -> {
            pool.execute(() -> {
                TaskInstance<String> taskInstance = new TaskInstance<>();
                taskInstance.setTaskState(TaskState.SUBMITTING);
                logger.info(String.format("the taskState of task %s is %s",
                        taskInstance, TaskState.SUBMITTING));
                taskInstance.setTaskParams(String.format("hello-i-am-the-client-message:%d", i));
                NettyClient nettyClient = new NettyClient("localhost", 8888, taskInstance);
                nettyClient.start();
            });
            //TODO 模拟多个任务之间的提交间隔，前一个NettyClient的socketChannel要在后续任务提交时
            //TODO 才能够被关闭，但任务的执行流程是不会被影响的，若任务的执行时间较长，那么可能会出现之前
            //TODO 多个任务的socketChannel没有关闭，需要等到任务提交间隔大于任务执行时间时才能关闭之前
            //TODO 的那些socketChannel，也可以在MasterExecTaskThread中等待，不过我们无法获知任务的
            //TODO 执行时间，所以没必要在MasterExecTaskThread中等待，因为机制会保证之前打开的sockChannel
            //TODO 会在后续某次提交任务时关闭
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("fail");
            }
        });
        pool.shutdown();
    }
}
