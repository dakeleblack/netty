package com.bitao.task.threadpool;

import com.bitao.task.util.Stopper;
import com.bitao.task.util.TaskCondition;
import com.bitao.task.util.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * MasterTaskExecThread线程负责从TaskCondition的taskQueue阻塞队列中拉取TaskInstance任务对象
 * 至WaitExecTaskQueue对象的阻塞队列中供Worker拉取执行任务
 */
public class MasterTaskExecThread implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(MasterTaskExecThread.class);
    //共享的TaskCondition对象
    private TaskCondition taskCondition;
    //该线程提交的任务，并监听该任务执行结束
    private TaskInstance taskInstance;

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

    public MasterTaskExecThread(TaskCondition taskCondition, TaskInstance taskInstance) {
        this.taskCondition = taskCondition;
        this.taskInstance = taskInstance;
    }

    @Override
    public Boolean call() throws InterruptedException {
        logger.info("master start to handle taskInstance");
        return submitExecTaskAndWaitFinished();
    }

    /**
     * 将从TaskCondition的taskQueue阻塞队列中获取的TaskInstance任务对象
     * 添加至WaitExecTaskQueue对象的阻塞队列中,添加成功后，该线程需要监控
     * 该任务的执行状态直至任务执行完成
     *
     * @return
     */
    public Boolean submitExecTaskAndWaitFinished() throws InterruptedException {
        try {
            // 添加TaskInstance至WaitExecTaskQueue对象的阻塞队列
            taskCondition.submitTask2ExecTaskQueue(taskInstance);
        } catch (Exception e) {
            logger.error(String.format("insert taskInstanceId %d to waitExecQueue fail:",
                    taskInstance.getTaskInstanceId()), e);
            return false;
        }
        // TODO 监控任务执行状态，直至task状态为success/failed/killed，否则此线程一直循环获取状态
        // TODO 问题：从哪里获取执行完后的TaskInstance，taskCondition中两个队列已经没有该TaskInstance了
        // TODO 若有数据库可以从数据库查询，可以再弄一个队列，专门用于存放提交执行的任务，执行完成后移除，
        // TODO 这边记录taskInstanceId，后面根据taskInstanceId从该队列查找指定TaskInstance，获取其状态
        while (Stopper.isRunning()) {
            TaskInstance task = taskCondition.getTaskInstanceById(taskInstance.getTaskInstanceId());
            if (task != null && task.getTaskState().isFinished()) {
                logger.info(String.format("the taskState of taskInstanceId %d is %s,remove this task" +
                        " from runningTasks now", taskInstance.getTaskInstanceId(), task.getTaskState()));
                taskCondition.removeTaskFromRunningTask(task.getTaskInstanceId());
                return true;
            }
            Thread.sleep(1000L);
        }
        return false;
    }
}
