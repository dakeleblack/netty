package com.bitao.task.msg;

import com.bitao.task.enums.MsgTypeEnum;
import com.bitao.task.util.TaskInstance;

public class AskMsg extends BaseMsg {

    private TaskInstance taskInstance;

    public AskMsg() {
        setType(MsgTypeEnum.ASK);
    }

    public TaskInstance getTaskInstance() {
        return taskInstance;
    }

    public void setTaskInstance(TaskInstance taskParam) {
        this.taskInstance = taskParam;
    }
}
