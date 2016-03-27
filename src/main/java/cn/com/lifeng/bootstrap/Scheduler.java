package cn.com.lifeng.bootstrap;

import cn.com.lifeng.job.*;
import cn.com.lifeng.util.JobStatus;

import java.util.concurrent.*;

/**
 * Created by lifeng on 16/3/26.
 */
public class Scheduler {
    private FileNameCache fileNameCache;
    private int threadNum;
    private ExecutorService executorService = null;
    private volatile JobStatus jobStatus = new JobStatus();
    private volatile boolean currentJobIsSuccess = true;

    private int batch = 1000;

    private VolatileInt[] firstColumnNumCache = new VolatileInt[batch];
    private VolatileInt[] secondColumnNumCache = new VolatileInt[batch];

    public Scheduler(FileNameCache fileNameCache, JobStatus jobStatus) {
        this.fileNameCache = fileNameCache;
        this.jobStatus = jobStatus;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    //1000000个文件几T数据，那1000个文件几G数据，ssd随机写磁盘性能70M，那么最多30s
    public void start() {
        fileNameCache.startListAllFile();
        int size = fileNameCache.getSize();
        if (size == 0) {
            return;
        }
        startExecutor();
        int batchTotal = (size / batch) + 1;
        int batchNum = 0;
        long currentLineNumber = jobStatus.getCurrentLineNumber();

        setFirstBatchRead();
        while (batchNum < batchTotal) {
            if (currentJobIsSuccess) {
                boolean readFirstCache = true;
                VolatileInt[] readCache = firstColumnNumCache;
                if (batchNum % 2 != 0) {
                    readFirstCache = false;
                    readCache = secondColumnNumCache;
                }
                int firstTaskWriteIdNum = batchNum * batch;
                int taskWriteNum = getCurrentBatchTaskNum(firstTaskWriteIdNum);
                int secondTaskReadIdNum = (batchNum + 1) * batch;
                int taskReadNum = getCurrentBatchTaskNum(secondTaskReadIdNum);

                CountDownLatch countDownLatch = new CountDownLatch(taskReadNum + taskWriteNum);
                for (int i = 0; i < taskWriteNum; i++) {
                    if (i < taskReadNum) {
                        FileRelatedTask readTask = new RetryTask(new FileLineNumberTask(fileNameCache.getInputFileNameByIndex(secondTaskReadIdNum + i)));
                        executorService.submit(new FileRelatedJob(readTask, countDownLatch, !readFirstCache, i));
                    }
                    FileRelatedTask writeTask = new RetryTask(new FileWriteTask(fileNameCache.getInputFileNameByIndex(firstTaskWriteIdNum + i), fileNameCache.getOutputFileNameByIndex(firstTaskWriteIdNum + i), currentLineNumber));
                    executorService.submit(new FileRelatedJob(writeTask, countDownLatch));
                    currentLineNumber += readCache[i].ele;
                }
                try {
                    countDownLatch.await(10, TimeUnit.MINUTES);
                    //全部成功 记录其状态
                    if(currentJobIsSuccess) {
                        writeJobStatus(firstTaskWriteIdNum + taskWriteNum - 1, currentLineNumber);
                        batchNum += 1;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    currentJobIsSuccess  = false;
                }
            } else {
                break;
            }
        }
        if(currentJobIsSuccess){
            stop();
        } else {
            stopNow();
        }
    }

    private void writeJobStatus(int fileId, long currentLineNumber) {
        jobStatus.setCurrentFileName(fileNameCache.getInputFileNameByIndex(fileId));
        jobStatus.setCurrentLineNumber(currentLineNumber);
        executorService.submit(new StatusWriteTask(JobStatus.clone(jobStatus)));
    }

    private int getCurrentBatchTaskNum(int taskStartId) {
        int taskNum = fileNameCache.getSize() - taskStartId;
        if (taskNum > batch) {
            taskNum = batch;
        } else if (taskNum < 0) {
            taskNum = 0;
        }
        return taskNum;
    }

    private void setFirstBatchRead() {
        int firstTaskIdNum = 0;
        int taskNum = fileNameCache.getSize();
        if (taskNum > batch) {
            taskNum = batch;
        }
        CountDownLatch countDownLatch = new CountDownLatch(taskNum);
        for (int i = 0; i < taskNum; i++) {
            FileRelatedTask readTask = new RetryTask(new FileLineNumberTask(fileNameCache.getInputFileNameByIndex(firstTaskIdNum + i)));
            executorService.submit(new FileRelatedJob(readTask, countDownLatch, true, i));
        }
        try {
            countDownLatch.await(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
            currentJobIsSuccess = false;
        }
    }

    private void startExecutor() {
        executorService = Executors.newFixedThreadPool(threadNum);
    }

    public void stop() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public void stopNow() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    private class VolatileInt {
        volatile int ele;

        VolatileInt(int ele) {
            this.ele = ele;
        }
    }

    private class StatusWriteTask implements Runnable {
        private JobStatus jobStatus;

        StatusWriteTask(JobStatus jobStatus) {
            this.jobStatus = jobStatus;
        }

        @Override
        public void run() {
            JobStatusCacheTask.setTaskStatus(jobStatus);
        }
    }

    private class FileRelatedJob implements Runnable {
        CountDownLatch jobLac;
        FileRelatedTask delegate;
        boolean firstCache;
        int index = -1;

        FileRelatedJob(FileRelatedTask fileRelatedTask, CountDownLatch jobLac) {
            this(fileRelatedTask, jobLac, true);
        }

        FileRelatedJob(FileRelatedTask fileRelatedTask, CountDownLatch jobLac, boolean firstCache) {
            this(fileRelatedTask, jobLac, firstCache, -1);
        }

        FileRelatedJob(FileRelatedTask fileRelatedTask, CountDownLatch jobLac, boolean firstCache, int index) {
            this.jobLac = jobLac;
            delegate = fileRelatedTask;
            this.index = index;
            this.firstCache = firstCache;
        }

        @Override
        public void run() {
            if (currentJobIsSuccess) {
                int result = delegate.start();
                if (result < 0) {
                    currentJobIsSuccess = false;
                } else if (index >= 0) {
                    if (firstCache) {
                        firstColumnNumCache[index] = new VolatileInt(result);
                    } else {
                        secondColumnNumCache[index] = new VolatileInt(result);
                    }
                }
            }
            jobLac.countDown();
        }
    }
}
