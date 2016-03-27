package cn.com.lifeng.bootstrap;

import cn.com.lifeng.job.*;
import cn.com.lifeng.util.JobStatus;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by lifeng on 16/3/26.
 */
public class Scheduler {
    private FileNameCache fileNameCache;
    private int threadNum;
    private ExecutorService executorService = null;
    private volatile JobStatus jobStatus;
    private volatile boolean currentJobIsSuccess = true;
    private int batch = 1000;
    private int[] firstColumnNumCache = new int[batch];
    private int[] secondColumnNumCache = new int[batch];

    public Scheduler(FileNameCache fileNameCache, JobStatus jobStatus, int threadNum) {
        this.fileNameCache = fileNameCache;
        this.jobStatus = jobStatus;
        this.threadNum = threadNum;
    }

    private void init() {
        fileNameCache.startListAllFile();
        startExecutor();
    }

    //1000000个文件几T数据，那1000个文件几G数据，ssd随机写磁盘性能70M，那么最多30s
    public void start() {
        init();
        int size = fileNameCache.getSize();
        if (size == 0) {
            return;
        }
        int batchTotal = (size / batch+1);
        int batchNum = 0;
        long currentLineNumber = jobStatus.getCurrentLineNumber();

        setFirstBatchRead();
        while (batchNum < batchTotal) {
            if (currentJobIsSuccess) {
                List<Future> jobs = new LinkedList<Future>();
                int[] readCache = firstColumnNumCache;
                int[] writeCache = secondColumnNumCache;
                if (batchNum % 2 != 0) {
                    readCache = secondColumnNumCache;
                    writeCache = firstColumnNumCache;
                }

                int firstTaskWriteIdNum = batchNum * batch;
                int taskWriteNum = getCurrentBatchTaskNum(firstTaskWriteIdNum);
                int secondTaskReadIdNum = (batchNum + 1) * batch;
                int taskReadNum = getCurrentBatchTaskNum(secondTaskReadIdNum);

                for (int i = 0; i < taskWriteNum; i++) {
                    if (i < taskReadNum) {
                        FileRelatedTask readTask = new RetryTask(new FileLineNumberTask(fileNameCache.getInputFileNameByIndex(secondTaskReadIdNum + i)));
                        jobs.add(executorService.submit(new FileRelatedCallable(readTask, i)));
                    }
                    FileRelatedTask writeTask = new RetryTask(new FileWriteTask(fileNameCache.getInputFileNameByIndex(firstTaskWriteIdNum + i), fileNameCache.getOutputFileNameByIndex(firstTaskWriteIdNum + i), currentLineNumber));
                    jobs.add(executorService.submit(new FileRelatedCallable(writeTask)));
                    currentLineNumber += readCache[i];
                }
                try {
                    for (Future<String> job : jobs) {
                        String result = job.get(4, TimeUnit.MINUTES);
                        processJobResult(result, writeCache);
                        if (!currentJobIsSuccess) break;
                    }
                    //全部成功 记录其状态
                    if (currentJobIsSuccess) {
                        writeJobStatus(firstTaskWriteIdNum + taskWriteNum - 1, currentLineNumber);
                        batchNum += 1;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    currentJobIsSuccess = false;
                }
            } else {
                break;
            }
        }
        if (currentJobIsSuccess) {
            stop();
        } else {
            stopNow();
        }
    }

    private void setFirstBatchRead() {
        List<Future> jobs = new LinkedList<Future>();
        int firstTaskIdNum = 0;
        int[] writeCache = firstColumnNumCache;
        int taskNum = getCurrentBatchTaskNum(firstTaskIdNum);

        for (int i = 0; i < taskNum; i++) {
            FileRelatedTask readTask = new RetryTask(new FileLineNumberTask(fileNameCache.getInputFileNameByIndex(firstTaskIdNum + i)));
            jobs.add(executorService.submit(new FileRelatedCallable(readTask, i)));
        }
        try {
            for (Future<String> job : jobs) {
                String result = job.get(4, TimeUnit.MINUTES);
                processJobResult(result, writeCache);
                if (!currentJobIsSuccess) break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            currentJobIsSuccess = false;
        }
    }

    private void writeJobStatus(int fileId, long currentLineNumber) {
        jobStatus.setCurrentFileName(fileNameCache.getInputFileNameByIndex(fileId));
        jobStatus.setCurrentLineNumber(currentLineNumber);
        executorService.submit(new StatusWriteTaskRunnable(JobStatus.clone(jobStatus)));
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

    private void processJobResult(String result, int[] cache) {
        switch (result) {
            case "-1":
                //表示读或者写失败
                currentJobIsSuccess = false;
                break;
            case "0":
                //表示写成功
                break;
            default:
                // 表示读成功
                String[] tmp = result.split("-");
                cache[Integer.parseInt(tmp[0])] = Integer.parseInt(tmp[1]);
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
}
