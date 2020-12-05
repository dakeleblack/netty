package com.bitao.task.queue;

import com.bitao.task.util.TaskInstance;

public interface WaitExecTaskQueue {

    boolean checkTaskExist(Long taskInstanceId);

    void addExecTask(TaskInstance taskInstance) throws InterruptedException;

    TaskInstance takeExecTask() throws InterruptedException;

    boolean removeExecTask(TaskInstance taskInstance);
}
