package com.bitao.task.threadpool;

import com.bitao.task.enums.TaskState;
import com.bitao.task.util.ShellCommandExecutor;
import com.bitao.task.util.TaskCondition;
import com.bitao.task.util.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

public class WorkerTaskExecThread implements Callable<Boolean> {
    private static final Logger logger = LoggerFactory.getLogger(MasterTaskExecThread.class);
    //共享的TaskCondition对象
    private TaskCondition taskCondition;
    //该线程执行的TaskInstance任务对象
    private TaskInstance taskInstance;
    //该线程内部启动执行任务的进程
    private Process process;

    public TaskCondition getTaskCondition() {
        return taskCondition;
    }

    public void setTaskCondition(TaskCondition taskCondition) {
        this.taskCondition = taskCondition;
    }

    public TaskInstance getTaskInstance() {
        return taskInstance;
    }

    public void setTaskInstance(TaskInstance taskInstance) {
        this.taskInstance = taskInstance;
    }

    public WorkerTaskExecThread(TaskCondition taskCondition, TaskInstance taskInstance) {
        this.taskCondition = taskCondition;
        this.taskInstance = taskInstance;
    }

    @Override
    public Boolean call() throws Exception {
        logger.info("worker start to handle taskInstance");
        taskInstance.setTaskState(TaskState.STARTING);
        //将执行中的任务添加到TaskCondition的runningTaskQueue中供后续的状态监控
        taskCondition.addTaskToRunningTask(taskInstance.getTaskInstanceId(), taskInstance);
        logger.info(String.format("the taskState of taskInstanceId %d is %s",
                taskInstance.getTaskInstanceId(), TaskState.STARTING));
        //拼接任务执行命令，本项目中的任务就是执行一个jar包
        //所以这里的执行命令就是：java -jar jarPath
        String command = ShellCommandExecutor.buildCommand((String) taskInstance.getTaskParams());
        logger.info(command);
        StringTokenizer stringTokenizer = new StringTokenizer(command);
        String[] commands = new String[stringTokenizer.countTokens()];
        for (int i = 0; stringTokenizer.hasMoreTokens(); i++) {
            commands[i] = stringTokenizer.nextToken();
        }
        try {
            //新起一个进程用于执行任务脚本命令
            ProcessBuilder processBuilder = new ProcessBuilder(commands).redirectErrorStream(true);
            //启动进程执行任务
            process = processBuilder.start();
            //设置任务状态为RUNNING
            taskInstance.setTaskState(TaskState.RUNNING);
            logger.info(String.format("the taskState of taskInstanceId %d is %s",
                    taskInstance.getTaskInstanceId(), TaskState.RUNNING));
            //获取子进程的输出
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                logger.info(line);
            }
            //等待任务执行结束
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                taskInstance.setTaskState(TaskState.SUCCESS);
            } else {
                taskInstance.setTaskState(TaskState.FAILED);
            }
        } catch (Exception e) {
            logger.error(String.format("execute taskInstanceId %d fail:",
                    taskInstance.getTaskInstanceId()), e);
            taskInstance.setTaskState(TaskState.FAILED);
            return false;
        } finally {
            if (process.isAlive()) {
                process.destroy();
            }
        }
        return true;
    }
}
