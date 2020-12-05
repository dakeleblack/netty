package com.bitao.task.threadpool;

import com.bitao.task.netty.NettyServer;
import com.bitao.task.util.Stopper;
import com.bitao.task.util.TaskCondition;
import com.bitao.task.util.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * MasterSingleThread线程就是在不断地拉取TaskCondition中的taskQueue阻塞队列中的待执行任务，
 * 然后将拉取到的任务放到WaitExecTaskQueue对象维护的阻塞队列中供Worker拉取执行
 * 由于NettyServer接收到NettyClient发送过来的消息后，就会把任务添加到TaskCondition的taskQueue
 * 阻塞队列中，所以这里就一直从taskQueue中拉取任务，注意的是TaskCondition对象是Master和Worker共享的
 * 这样才能传输任务实例
 */
public class MasterSingleThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MasterSingleThread.class);

    private static final int coreMasterExecThreadNum = 10;

    private static final int maxMasterExecThreadNum = 20;

    //创建线程池，核心池容量为10，10个线程同时从TaskCondition的taskQueue阻塞队列中拉取待执行任务添加至
    //WaitExecTaskQueue对象的阻塞队列中，若这里不用线程池，则只有一个线程做上述那些事情，降低了执行效率
    private ThreadPoolExecutor masterThreadPool = new ThreadPoolExecutor(
            coreMasterExecThreadNum,
            maxMasterExecThreadNum,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(16));
    //共享的TaskCondition对象
    private TaskCondition taskCondition;

    private NettyServer nettyServer;

    //保存正在执行任务的线程对象，key为正在执行任务的线程，value为该线程的异步执行结果
    private Map<MasterTaskExecThread, Future<Boolean>> activeThread = new ConcurrentHashMap<>();

    public MasterSingleThread(NettyServer nettyServer, TaskCondition taskCondition) {
        this.nettyServer = nettyServer;
        this.taskCondition = taskCondition;
    }

    @Override
    public void run() {
        // 一直循环，线程池中的每个线程拉取taskCondition对象的taskQueue队列中的任务，
        // 内部一直在轮询taskCondition中的taskQueue队列，等待拉取任务，若没有待执行的任务，则阻塞等待
        // 一旦某个线程的任务执行结束，若taskQueue中有新的任务，则空闲线程执行提交执行任务
        while (Stopper.isRunning()) {
            //先将activeThread集合中执行结束的线程对象移除
            activeThread.keySet().forEach(masterTaskExecThread -> {
                Future<Boolean> result = activeThread.get(masterTaskExecThread);
                TaskInstance taskInstance = masterTaskExecThread.getTaskInstance();
                //判断线程是否执行结束，执行结束的任务关闭对应NettyClient的channel
                if (result.isDone()) {
                    activeThread.remove(masterTaskExecThread);
                    //TODO 这里将执行完成的线程所对应的nettyClient的channel关闭，
                    //TODO 其实最好的是应该在nettyClient发送完ASK任务参数后，这个channel就可以关闭了
                    nettyServer.getChannelByClientId(taskInstance.getClientId()).close().addListener(event ->
                            logger.info(String.format("netty channel close:%s %s",
                                    taskInstance.getClientId(), event)));
                }
            });
            //获取线程池当前正在执行任务的线程数,若已达线程池上限,则等待直至有空闲线程
            int activeThreadCount = masterThreadPool.getActiveCount();
            if (activeThreadCount >= maxMasterExecThreadNum) {
                logger.error(String.format("当前提交任务的线程数目为%d,已达线程池上限%d",
                        activeThreadCount, maxMasterExecThreadNum));
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            //若线程池有空余线程，则从taskQueue中拉取待执行任务进行提交
            TaskInstance taskInstance;
            try {
                //从TaskCondition的taskQueue阻塞队列中拉取任务，若taskQueue中没有任务，则会阻塞于此
                taskInstance = taskCondition.getTaskQueue().take().getValue();
            } catch (InterruptedException e) {
                logger.error("take waiting task from taskQueue fail:", e);
                continue;
            }
            //有一个任务过来就实例化一个任务执行线程对象执行任务
            MasterTaskExecThread masterTaskExecThread = new MasterTaskExecThread(taskCondition, taskInstance);
            //执行该线程对象，负责从TaskCondition的taskQueue阻塞队列中拉取任务添加至WaitExecTaskQueue
            //对象的阻塞队列中供后续worker拉取,返回异步执行结果。线程池中的每个线程要监控对应任务的状态直至该
            //任务执行完成后，该线程才执行结束，在这期间可能会有新的任务来，所以这里使用线程池来处理任务提交，
            //可以提高任务执行的效率，否则只有一个线程就要一直等待直至任务完成，后续线程池的某个线程执行结束后，
            //空闲线程可以执行新来的masterTaskExecThread线程对象
            Future<Boolean> result = masterThreadPool.submit(masterTaskExecThread);
            //将这个线程对象加入activeThread集合中，putIfAbsent是原子操作
            activeThread.putIfAbsent(masterTaskExecThread, result);
        }
    }
}
