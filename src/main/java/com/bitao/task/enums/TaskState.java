package com.bitao.task.enums;

public enum TaskState {
    SUBMITTING,
    WAITING,
    STARTING,
    RUNNING,
    KILLED,
    SUCCESS,
    FAILED;

    public boolean isFinished() {
        return isSuccess() || isFailed() || isKilled();
    }

    public boolean isSuccess() {
        return this == SUCCESS;
    }

    public boolean isFailed() {
        return this == FAILED;
    }

    public boolean isKilled() {
        return this == KILLED;
    }
}
