package com.bitao.task.util;

import com.sun.deploy.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ShellCommandExecutor {

    public static String buildCommand(String args) {
        List<String> commands = new ArrayList<>();
        commands.add(CommandPrefixUtil.JAVA_COMMAND);
        commands.add(CommandPrefixUtil.SPLIT + CommandPrefixUtil.JAR_COMMAND);
        commands.add(CommandPrefixUtil.JAR_PATH);
        commands.add(args);
        return StringUtils.join(commands, CommandPrefixUtil.BLANK);
    }
}
