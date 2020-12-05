package com.bitao.task.msg;

import com.bitao.task.enums.MsgTypeEnum;

public class PingMsg extends BaseMsg {
    public PingMsg() {
        setType(MsgTypeEnum.PING);
    }
}
