package com.bitao.task.scheduler;

import com.bitao.task.threadpool.WorkerSingleThread;
import com.bitao.task.util.Stopper;
import com.bitao.task.util.TaskCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class WorkerServer {
    private static final Logger logger = LoggerFactory.getLogger(WorkerServer.class);

    private static final ThreadFactory threadFactory = r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("worker-single-thread-");
        return thread;
    };

    //单线程池，与MasterServer一样，执行WorkerSingleThread线程对象，该线程对象内又有一个线程池
    //用于真正地执行任务
    private static final ExecutorService workerSingleExecutorService = Executors.newSingleThreadExecutor(threadFactory);

    //共享的TaskCondition
    private TaskCondition taskCondition;

    public WorkerServer(TaskCondition taskCondition) {
        this.taskCondition = taskCondition;
    }

    /**
     * workerServer的执行方法
     */
    public void run() {
        //实例化WorkerSingleThread线程对象，内部一直在循环，从WaitExecTaskQueue对象的阻塞队列中拉取任务执行
        WorkerSingleThread workerSingleThread = new WorkerSingleThread(taskCondition);
        workerSingleExecutorService.execute(workerSingleThread);
        logger.info("start worker success");
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public void stop() {
        if (Stopper.isStopped()) {
            return;
        }
        Stopper.stop();
        //关闭线程池以及NettyServer
        try {
            workerSingleExecutorService.shutdown();
            workerSingleExecutorService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("close workerServer fail:", e);
        }
    }
}
