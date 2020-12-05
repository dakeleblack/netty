package com.bitao.task.msg;

import com.bitao.task.enums.MsgTypeEnum;

import java.io.Serializable;

public class BaseMsg implements Serializable {
    private static final long serialVersionUID = 1L;

    //消息类型
    private MsgTypeEnum type;

    //消息的唯一id
    private String msgId;

    public MsgTypeEnum getType() {
        return type;
    }

    public void setType(MsgTypeEnum type) {
        this.type = type;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
}
