package com.bitao.task.queue;

import com.bitao.task.util.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

public class WaitExecTaskQueueImpl implements WaitExecTaskQueue {

    private static final Logger logger = LoggerFactory.getLogger(WaitExecTaskQueueImpl.class);

    //TODO volatile用于单例模式的设计
    private static volatile WaitExecTaskQueueImpl waitExecTaskQueueImpl;

    private ArrayBlockingQueue<TaskInstance> waitExecTaskQueue = new ArrayBlockingQueue<>(10);

    private WaitExecTaskQueueImpl() {
    }

    public static WaitExecTaskQueueImpl getInstance() {
        if (waitExecTaskQueueImpl == null) {
            synchronized (WaitExecTaskQueueImpl.class) {
                //todo 双重校验
                if (waitExecTaskQueueImpl == null) {
                    waitExecTaskQueueImpl = new WaitExecTaskQueueImpl();
                }
            }
        }
        return waitExecTaskQueueImpl;
    }

    @Override
    public boolean checkTaskExist(Long taskInstanceId) {
        for (TaskInstance taskInstance : waitExecTaskQueueImpl.waitExecTaskQueue) {
            if (taskInstance.getTaskInstanceId() == taskInstanceId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addExecTask(TaskInstance taskInstance) throws InterruptedException {
        waitExecTaskQueueImpl.waitExecTaskQueue.put(taskInstance);
    }

    @Override
    public TaskInstance takeExecTask() throws InterruptedException {
        return waitExecTaskQueueImpl.waitExecTaskQueue.take();
    }

    @Override
    public boolean removeExecTask(TaskInstance taskInstance) {
        for (TaskInstance taskInstance1 : waitExecTaskQueueImpl.waitExecTaskQueue) {
            if (taskInstance.getTaskInstanceId() == taskInstance1.getTaskInstanceId()) {
                return waitExecTaskQueueImpl.waitExecTaskQueue.remove(taskInstance1);
            }
        }
        return false;
    }
}
