package cn.com.lifeng.bootstrap;

import cn.com.lifeng.util.JobStatus;

import java.io.*;

/**
 * Created by lifeng on 16/3/26.
 */
public class JobStatusCacheTask {
    //用于记录任务处理状态，方便任务重启,减少应用
    public static JobStatus getTaskStatus(String statusFilePath) {
        JobStatus jobStatus = new JobStatus();
        jobStatus.setStatusFilePath(statusFilePath);
        String[] taskStatus = null;
        FileReader fileReader = null;
        BufferedReader fileBuffer = null;
        try {
            fileReader = new FileReader(statusFilePath);
            //初始化其字节大小为30字节，默认缓存大小8k字节
            fileBuffer = new BufferedReader(fileReader, 30);
            taskStatus = fileBuffer.readLine().split("-");
            jobStatus.setCurrentFileName(taskStatus[0]);
            jobStatus.setCurrentLineNumber(Double.parseDouble(taskStatus[1]));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileBuffer != null) {
                    fileBuffer.close();
                }
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return jobStatus;
    }

    public static void setTaskStatus(JobStatus jobStatus) {
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;
        try {
            //初始化其字节大小为30字节，默认缓存大小8k字节
            fileWriter = new FileWriter(jobStatus.getStatusFilePath());
            bufferedWriter = new BufferedWriter(fileWriter, 30);
            bufferedWriter.write(jobStatus.getCurrentFileName() + "-"+jobStatus.getCurrentLineNumber());
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
                if (fileWriter != null) {
                    fileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
