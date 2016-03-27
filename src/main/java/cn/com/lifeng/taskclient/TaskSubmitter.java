package cn.com.lifeng.taskclient;

import cn.com.lifeng.taskserver.Scheduler;
import cn.com.lifeng.taskserver.StatusWriteTaskRunnable;
import cn.com.lifeng.util.JobStatus;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/28.
 */
public class TaskSubmitter {
    static Logger logger = Logger.getLogger(TaskSubmitter.class.getName());

    private FileNameCache fileNameCache;
    private JobStatus jobStatus;
    private Scheduler scheduler;
    private boolean currentJobIsSuccess = true;
    private int batch = 1000;
    private int[] firstColumnNumCache = new int[batch];
    private int[] secondColumnNumCache = new int[batch];

    public TaskSubmitter(FileNameCache fileNameCache, JobStatus jobStatus) {
        this.fileNameCache = fileNameCache;
        this.jobStatus = jobStatus;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    private void init() {
        if (jobStatus.hasInitialed()) {
            logger.info("Initial job status(filename:linenumber) is------" + jobStatus.toString());
        } else {
            logger.warn("Job is not initialed, it will start the job from beginning");
        }
        fileNameCache.startListAllFile();
        scheduler.start();
        logger.info("Begin submit task");
    }

    //1000000个文件几T数据，那1000个文件几G数据，ssd随机写磁盘性能70M，那么最多30s
    public void start() {
        init();
        int size = fileNameCache.getSize();
        if (size > 0) {
            int batchTotal = (size / batch + 1);
            int batchNum = 0;
            long currentLineNumber = jobStatus.getCurrentLineNumber();
            setFirstBatchRead();
            while (batchNum < batchTotal) {
                if (!currentJobIsSuccess) break;
                currentLineNumber = startBatchReadAndWrite(batchNum, currentLineNumber);
                batchNum += 1;
            }
        }
        stopJob();
    }

    private void setFirstBatchRead() {
        List<Future> jobs = new LinkedList<Future>();
        int firstTaskIdNum = 0;
        int[] writeCache = firstColumnNumCache;
        int taskNum = getCurrentBatchTaskNum(firstTaskIdNum);
        for (int i = 0; i < taskNum; i++) {
            try {
                FileRelatedTask readTask = new FileLineNumberTask(fileNameCache.getInputFileNameByIndex(firstTaskIdNum + i), i);
                jobs.add(scheduler.submitFileJob(readTask, 1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            for (Future<String> job : jobs) {
                String result = job.get(1, TimeUnit.MINUTES);
                processJobResult(result, writeCache);
                if (!currentJobIsSuccess) break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            currentJobIsSuccess = false;
        }
    }

    private long startBatchReadAndWrite(int batchNum, long currentLineNumber) {
        List<Future> jobs = new LinkedList<Future>();
        int[] readCache = firstColumnNumCache;
        int[] writeCache = secondColumnNumCache;
        if (batchNum % 2 != 0) {
            readCache = secondColumnNumCache;
            writeCache = firstColumnNumCache;
        }
        //firstTaskWriteIdNum 对应filecachename的任务索引id，taskWriteNum对应该batch的任务数量，对应着columnNumCache的索引
        int firstTaskWriteIdNum = batchNum * batch;
        int taskWriteNum = getCurrentBatchTaskNum(firstTaskWriteIdNum);
        int secondTaskReadIdNum = (batchNum + 1) * batch;
        int taskReadNum = getCurrentBatchTaskNum(secondTaskReadIdNum);
        try {
            for (int i = 0; i < taskWriteNum; i++) {
                if (i < taskReadNum) {
                    FileRelatedTask readTask = new FileLineNumberTask(fileNameCache.getInputFileNameByIndex(secondTaskReadIdNum + i), i);
                    jobs.add(scheduler.submitFileJob(readTask, 1));
                }
                // write task 获取两个资源
                FileRelatedTask writeTask = new FileWriteTask(fileNameCache.getInputFileNameByIndex(firstTaskWriteIdNum + i), fileNameCache.getOutputFileNameByIndex(firstTaskWriteIdNum + i), currentLineNumber);
                jobs.add(scheduler.submitFileJob(writeTask, 2));
                currentLineNumber += readCache[i];
            }
            // 获取结果
            for (Future<String> job : jobs) {
                String result = job.get(1, TimeUnit.MINUTES);
                processJobResult(result, writeCache);
                if (!currentJobIsSuccess) break;
            }
            //全部成功 记录其状态
            if (currentJobIsSuccess) writeJobStatus(firstTaskWriteIdNum + taskWriteNum - 1, currentLineNumber);
        } catch (Exception e) {
            e.printStackTrace();
            currentJobIsSuccess = false;
        }
        return currentLineNumber;
    }

    private void writeJobStatus(int fileId, long currentLineNumber) {
        jobStatus.setCurrentFileName(fileNameCache.getInputFileNameByIndex(fileId));
        jobStatus.setCurrentLineNumber(currentLineNumber);
        logger.info("Current process: " + jobStatus.toString());
        scheduler.submitCommonJob(new StatusWriteTaskRunnable(JobStatus.clone(jobStatus)));
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

    private void stopJob() {
        scheduler.stop();
        if (currentJobIsSuccess) {
            logger.info("Job is finished");
        } else {
            logger.error("Job is failed");
        }
    }
}
