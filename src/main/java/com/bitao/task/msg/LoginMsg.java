package com.bitao.task.msg;

import com.bitao.task.enums.MsgTypeEnum;

public class LoginMsg extends BaseMsg {
    public LoginMsg() {
        setType(MsgTypeEnum.LOGIN);
    }
}
