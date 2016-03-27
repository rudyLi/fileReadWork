package cn.com.lifeng.taskserver;

import cn.com.lifeng.taskclient.FileRelatedTask;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/27.
 */
public class RetryTask implements FileRelatedTask {
    private final static String FAIL  = "-1";
    private FileRelatedTask delegate;
    private int retryTimes;

    @Override
    public String start() {
        String result = FAIL;
        while (retryTimes > 0) {
            result = delegate.start();
            retryTimes -= 1;
            if (!result.equals(FAIL)) break;
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

    @Override
    public void setBuffer(ByteBuffer[] buffer) {
        delegate.setBuffer(buffer);
    }

    public RetryTask(FileRelatedTask fileRelatedTask) {
        this(fileRelatedTask, 3);
    }

    public RetryTask(FileRelatedTask fileRelatedTask, int retryTimes) {
        delegate = fileRelatedTask;
        this.retryTimes = retryTimes;
    }
}
