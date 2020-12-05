package com.bitao.task.util;

import com.bitao.task.exception.TaskException;
import com.bitao.task.queue.WaitExecTaskQueue;
import com.bitao.task.queue.WaitExecTaskQueueImpl;
import com.bitao.task.enums.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TaskCondition {
    private static final Logger logger = LoggerFactory.getLogger(TaskCondition.class);

    // 自动生成任务id，这里使用一个线程安全的原子计数类
    private static AtomicLong id = new AtomicLong(1);

    // 存放待执行的任务队列，队列容量为100，key为任务id，value为构造的待执行任务
    private static volatile ArrayBlockingQueue<Tuple2<Long, TaskInstance>> taskQueue = new ArrayBlockingQueue<>(100);

    // 存放执行中任务的队列
    private static volatile ConcurrentHashMap<Long, TaskInstance> runningTasks = new ConcurrentHashMap<>(16);

    // 存放待执行任务的队列，与taskQueue有所区别，该队列中的task是从taskQueue中拉取的，即taskQueue队列是用于
    // 存放用户提交执行的任务，而waitExecTaskQueue对象内部维护的队列则是真正的等待执行的任务,供后续worker执行
    private static volatile WaitExecTaskQueue waitExecTaskQueue;

    public TaskCondition() {
        //初始化待执行任务队列
        waitExecTaskQueue = WaitExecTaskQueueImpl.getInstance();
    }

    public ConcurrentHashMap<Long, TaskInstance> getRunningTasks() {
        return runningTasks;
    }

    public ArrayBlockingQueue<Tuple2<Long, TaskInstance>> getTaskQueue() {
        return taskQueue;
    }

    public WaitExecTaskQueue getWaitExecTaskQueue() {
        return waitExecTaskQueue;
    }

    /**
     * 添加TaskInstance待执行任务至taskQueue任务队列中
     *
     * @param taskInstance 待执行任务
     * @param clientId     此次提交任务对应的唯一标识id
     * @return 任务id
     */
    public long addTask(TaskInstance taskInstance, String clientId) throws InterruptedException {
        //构造TaskInstance任务对象
        taskInstance.setTaskInstanceId(id.getAndIncrement());
        //设置该任务对于的消息id，即对于了某个客户端，后续需根据消息id找到对应客户端
        taskInstance.setClientId(clientId);
        taskInstance.setTaskState(TaskState.WAITING);
        logger.info(String.format("the taskState of taskInstanceId %d is %s", taskInstance.getTaskInstanceId(), TaskState.WAITING));
        //如果队列已满，put操作会阻塞
        taskQueue.put(new Tuple2<>(taskInstance.getTaskInstanceId(), taskInstance));
        return taskInstance.getTaskInstanceId();
    }

    /**
     * 添加TaskInstance至waitExecTaskQueue队列中供后续worker执行
     *
     * @param taskInstance
     * @throws InterruptedException
     */
    public void submitTask2ExecTaskQueue(TaskInstance taskInstance) throws Exception {
        if (taskInstance.getTaskState() != TaskState.WAITING) {
            logger.error(String.format("the taskState %s of taskInstanceId %d is wrong",
                    taskInstance.getTaskState(), taskInstance.getTaskInstanceId()));
            throw new TaskException(String.format("the taskState %s of taskInstanceId %d is wrong",
                    taskInstance.getTaskState(), taskInstance.getTaskInstanceId()));
        }
        logger.info(String.format("taskInstanceId %d ready to insert to waitExecTaskQueue", taskInstance.getTaskInstanceId()));
        waitExecTaskQueue.addExecTask(taskInstance);
        logger.info(String.format("taskInstanceId %d insert to waitExecTaskQueue successfully", taskInstance.getTaskInstanceId()));
    }

    public TaskInstance getTaskInstanceById(Long taskInstanceId) {
        if (runningTasks.containsKey(taskInstanceId)) {
            return runningTasks.get(taskInstanceId);
        }
        return null;
    }

    public void addTaskToRunningTask(Long taskInstanceId, TaskInstance taskInstance) {
        runningTasks.putIfAbsent(taskInstanceId, taskInstance);
    }

    public void removeTaskFromRunningTask(Long taskInstanceId) {
        runningTasks.remove(taskInstanceId);
    }
}
