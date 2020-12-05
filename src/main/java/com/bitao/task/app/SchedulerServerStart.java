package com.bitao.task.app;

import com.bitao.task.scheduler.MasterServer;
import com.bitao.task.scheduler.WorkerServer;
import com.bitao.task.util.TaskCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务端起了两个线程池分别处理：
 * MasterServer:负责处理连接的建立,以及处理初始任务参数,提交处理后的任务至WaitExecThreadQueue维护的队列中
 *              等待worker从队列中拉取任务执行
 * WorkerServer:负责真正处理任务，不断从WaitExecThreadQueue维护的队列中拉取任务，一旦有任务即可执行
 */
public class SchedulerServerStart {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerServerStart.class);

    public static void main(String[] args) {
        //实例化TaskCondition对象，维护taskInstance任务对象，所以taskCondition是共享的
        TaskCondition taskCondition = new TaskCondition();
        MasterServer masterServer = new MasterServer(taskCondition);
        WorkerServer workerServer = new WorkerServer(taskCondition);
        masterServer.run();
        workerServer.run();
    }

}
