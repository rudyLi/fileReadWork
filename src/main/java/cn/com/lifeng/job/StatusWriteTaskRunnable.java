package cn.com.lifeng.job;

import cn.com.lifeng.util.JobStatus;

/**
 * Created by lifeng on 16/3/27.
 */
public class StatusWriteTaskRunnable implements Runnable{
    private JobStatus jobStatus;

    public StatusWriteTaskRunnable(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    @Override
    public void run() {
        JobStatusCacheTask.setTaskStatus(jobStatus);
    }
}
