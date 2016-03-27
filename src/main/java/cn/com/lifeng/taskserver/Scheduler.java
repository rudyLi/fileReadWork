package cn.com.lifeng.taskserver;

import cn.com.lifeng.taskclient.*;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * Created by lifeng on 16/3/26.
 */
public class Scheduler {
    static Logger logger = Logger.getLogger(Scheduler.class.getName());

    private ResourceManager resource;

    private final static int MEMORY_LIMIT = 1024 * 1024 * 30;
    private int threadNum = 10;
    private int bufferSize = 1024 * 1024;

    //scheduler需要保留的状态
    private ExecutorService executorService = null;

    public Scheduler(int threadNum, int bufferSize) {
        setThreadNum(threadNum);
        setBufferSize(bufferSize);
    }

    public void setThreadNum(int threadNum) {
        int currentMaxThread = MEMORY_LIMIT / (bufferSize * 2);
        if (threadNum > currentMaxThread) {
            this.threadNum = currentMaxThread;
            logger.warn("ThreadNum is too large,will change the threadNum to " + currentMaxThread);
        } else {
            this.threadNum = threadNum;
        }
    }

    public void setBufferSize(int bufferSize) {
        int currentMaxBufferSize = MEMORY_LIMIT / (threadNum * 2);
        if (bufferSize > currentMaxBufferSize) {
            this.bufferSize = currentMaxBufferSize;
            logger.warn("BufferSize is too large,will change the bufferSize to " + currentMaxBufferSize);
        } else {
            this.bufferSize = bufferSize;
        }
    }

    public void start() {
        initResourceManager();
        executorService = Executors.newFixedThreadPool(threadNum);
        logger.info("Scheduler init success and start running");
    }

    private void initResourceManager() {
        //这儿需申请至少两个资源，因为写任务的时候需要两个buffer
        int capacity = 2 * threadNum;
        resource = new FIFOResourceManager(capacity, bufferSize);
    }

    public Future submitFileJob(FileRelatedTask fileRelatedTask, int needBufferNum) throws InterruptedException {
        FileRelatedTask fileTask = new RetryTask(fileRelatedTask);
        ByteBuffer[] byteBuffer = new ByteBuffer[needBufferNum];
        for (int i = 0; i < needBufferNum; i++) {
            byteBuffer[i] = resource.get();
        }
        return executorService.submit(new FileRelatedCallable(fileTask, resource, byteBuffer));
    }

    public void submitCommonJob(Runnable runnable){
        executorService.submit(runnable);
    }

    public void stop() {
        if (executorService != null) executorService.shutdown();
        resource.release();
    }
}
