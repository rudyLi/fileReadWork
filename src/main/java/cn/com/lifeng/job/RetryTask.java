package cn.com.lifeng.job;

import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/27.
 */
public class RetryTask implements FileRelatedTask{
    private FileRelatedTask delegate;
    private int retryTimes;

    @Override
    public int start() {
        int result = 0;
        while (retryTimes > 0) {
            result = delegate.start();
            retryTimes -= 1;
            if (result>=0) break;
            //最后一次时不需要sleep
            if (retryTimes > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    public RetryTask(FileRelatedTask fileRelatedTask){
        this(fileRelatedTask,3);
    }
    public RetryTask(FileRelatedTask fileRelatedTask,int retryTimes){
        delegate = fileRelatedTask;
        this.retryTimes = retryTimes;
    }
}
