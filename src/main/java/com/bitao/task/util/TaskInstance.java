package com.bitao.task.util;

import com.bitao.task.enums.TaskState;

import java.io.Serializable;
import java.util.Objects;

public class TaskInstance<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private long taskInstanceId;

    private T taskParams;

    private String clientId;

    private TaskState taskState;

    public long getTaskInstanceId() {
        return taskInstanceId;
    }

    public void setTaskInstanceId(long taskInstanceId) {
        this.taskInstanceId = taskInstanceId;
    }

    public T getTaskParams() {
        return taskParams;
    }

    public void setTaskParams(T taskParams) {
        this.taskParams = taskParams;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public TaskState getTaskState() {
        return taskState;
    }

    public void setTaskState(TaskState taskState) {
        this.taskState = taskState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskInstance<?> that = (TaskInstance<?>) o;
        return taskInstanceId == that.taskInstanceId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskInstanceId, taskParams, clientId, taskState);
    }
}
