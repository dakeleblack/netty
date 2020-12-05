package com.bitao.task.threadpool;

import com.bitao.task.util.Stopper;
import com.bitao.task.util.TaskCondition;
import com.bitao.task.util.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class WorkerSingleThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WorkerSingleThread.class);

    private static final int coreWorkerExecThreadNum = 10;

    private static final int maxWorkerExecThreadNum = 20;

    //共享的TaskCondition对象,从该对象的WaitExecTaskQueue的阻塞队列中拉取任务执行
    private TaskCondition taskCondition;

    //线程池,核心容量为10,用于真正执行任务
    private ThreadPoolExecutor workerThreadPool = new ThreadPoolExecutor(
            coreWorkerExecThreadNum,
            maxWorkerExecThreadNum,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100));

    public WorkerSingleThread(TaskCondition taskCondition) {
        this.taskCondition = taskCondition;
    }

    @Override
    public void run() {
        //内部一直循环拉取waitExecTaskQueue中的待执行任务
        while (Stopper.isRunning()) {
            //若当前线程池中执行任务的线程已达上限，则等待直至有空闲线程
            int activeThreadCount = workerThreadPool.getActiveCount();
            if (activeThreadCount >= maxWorkerExecThreadNum) {
                logger.error(String.format("当前执行任务的线程数目为%d,已达线程池上限%d",
                        activeThreadCount, maxWorkerExecThreadNum));
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            //若线程池有空闲线程，则从waitExecTaskQueue中拉取任务，放到线程池中执行
            TaskInstance taskInstance;
            try {
                //从TaskCondition的taskQueue队列中拉取一个待执行任务TaskInstance对象,
                //若没有待执行任务,则该线程阻塞于此
                taskInstance = taskCondition.getWaitExecTaskQueue().takeExecTask();
            } catch (InterruptedException e) {
                logger.error("take exec task from waitExecTaskQueue fail:", e);
                continue;
            }
            WorkerTaskExecThread workerTaskExecThread = new WorkerTaskExecThread(taskCondition, taskInstance);
            workerThreadPool.submit(workerTaskExecThread);
        }
    }

}
