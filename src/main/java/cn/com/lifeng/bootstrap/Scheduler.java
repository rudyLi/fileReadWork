package cn.com.lifeng.bootstrap;

import cn.com.lifeng.util.FileNameUtil;
import cn.com.lifeng.util.JobStatus;

import java.util.concurrent.*;

/**
 * Created by lifeng on 16/3/26.
 */
public class Scheduler {
    private FileNameUtil fileNameUtil;
    private int threadNum;
    private ScheduledExecutorService scheduleService = Executors.newScheduledThreadPool(1);
    private ExecutorService executorService = null;
    private volatile JobStatus jobStatus =  new JobStatus();
    private volatile boolean currentSuccess = true;

    private int batch = 1000;
    private VolatileInt[] volatileIntArray = new VolatileInt[batch];

    public Scheduler(FileNameUtil fileNameUtil, JobStatus jobStatus){
        this.fileNameUtil = fileNameUtil;
        this.jobStatus = jobStatus;
    }
    public void setJobStatus(JobStatus jobStatus){
        this.jobStatus = jobStatus;
    }

    public void setThreadNum(int threadNum){
        this.threadNum = threadNum;
    }
    //1000000个文件几T数据，那1000个文件几G数据，ssd随机写磁盘性能70M，那么最多
    public void start(){
        startExecutor();
        int size = fileNameUtil.getSize();
        int batchTotal = (size/batch)+1;
        int batchNum = 0;
        double currentLineNumber = jobStatus.getCurrentLineNumber();

        while (batchNum<batchTotal){
            int firstTaskIdNum = batchNum*batch;
            int taskNum = size-firstTaskIdNum;

            CountDownLatch countDownLatch =  new CountDownLatch(taskNum);
            for(int i=0;i<taskNum;i++){
                executorService.submit(new FileLineNumberJob(i, fileNameUtil.getInputFileNameByIndex(firstTaskIdNum + i),countDownLatch));
            }
            try {
                countDownLatch.await(10,TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
            CountDownLatch writeCountDown = new CountDownLatch(taskNum);
            for(int i=0;i<taskNum;i++){
                currentLineNumber += volatileIntArray[i].ele;
                executorService.submit(new FileWriteJob(fileNameUtil.getInputFileNameByIndex(firstTaskIdNum+i), fileNameUtil.getOutputFileNameByIndex(firstTaskIdNum+i),currentLineNumber,writeCountDown));
            }
            try {
                writeCountDown.await(10,TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
           jobStatus.setCurrentLineNumber(currentLineNumber);
           jobStatus.setCurrentFileName(fileNameUtil.getFileNameByIndex(firstTaskIdNum+taskNum-1));
        }
        stop();
    }
    private void startExecutor(){
        scheduleService.scheduleAtFixedRate(new StatusScheduleWriteTask(),10, 60, TimeUnit.SECONDS);
        executorService = Executors.newFixedThreadPool(threadNum);
    }
    public void stop(){
        if(executorService!=null) {
            executorService.shutdown();
        }
        scheduleService.shutdown();
    }
    private class VolatileInt{
        VolatileInt(int ele){
            this.ele = ele;
        }
        volatile int ele = 0;
    }

    private class StatusScheduleWriteTask implements Runnable{
        @Override
        public void run() {
            JobStatusCacheTask.setTaskStatus(jobStatus);
        }
    }
    private class FileLineNumberJob implements Runnable{
        int index;
        String fileName;
        CountDownLatch jobLac;
        FileLineNumberJob(int index,String fileName,CountDownLatch jobLac){
            this.index = index;
            this.fileName = fileName;
            this.jobLac = jobLac;
        }
        @Override
        public void run() {
            if(currentSuccess){
                int lineNumber = new FileLineNumberTask(fileName).getLineNumber();
                if(lineNumber<0){
                    currentSuccess =false;
                } else {
                    volatileIntArray[index] = new VolatileInt(lineNumber);
                }
            }
            jobLac.countDown();
        }
    }

    private class FileWriteJob implements Runnable{
        String inputFile;
        String outputFile;
        double fileLineNumber;
        CountDownLatch jobLoc;
        FileWriteJob(String inputFile,String outputFile,double fileLineNumber, CountDownLatch jobLoc){
            this.fileLineNumber = fileLineNumber;
            this.inputFile = inputFile;
            this.outputFile = outputFile;
            this.jobLoc = jobLoc;
        }
        @Override
        public void run() {
            if(currentSuccess){
                boolean writeSuccess = new FileWriteTask(inputFile,outputFile,fileLineNumber).startWrite();
                if(!writeSuccess){
                    currentSuccess =false;
                }
            }
            jobLoc.countDown();
        }
    }
}
