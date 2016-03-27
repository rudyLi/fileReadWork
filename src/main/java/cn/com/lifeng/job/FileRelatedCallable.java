package cn.com.lifeng.job;

import java.util.concurrent.Callable;

/**
 * Created by lifeng on 16/3/27.
 */
public class FileRelatedCallable implements Callable<String> {
    private static final String FAIL = "-1";
    private static final String WRITE_SUCCESS = "0";

    private FileRelatedTask delegate;
    //ugly index 一方面记录该read结果应该保存缓存的位置，一方面可以区分是读任务或者写任务 index -1 表示该任务为写任务
    int index = -1;

    public FileRelatedCallable(FileRelatedTask fileRelatedTask) {
        this(fileRelatedTask, -1);
    }

    public FileRelatedCallable(FileRelatedTask fileRelatedTask, int index) {
        delegate = fileRelatedTask;
        this.index = index;
    }
    @Override
    // 任务失败返回结果为"-1",读任务返回为index 加结果，
    public String call() {
        String result = FAIL;
        int taskResult = delegate.start();
        //读任务且结果返回成功，则返回行数以及对应的cache索引
        if (index >= 0 && taskResult>= 0) {
            result = index + "-" + taskResult;
        } else if (taskResult == 0) {
            result = WRITE_SUCCESS;
        }
        return result;
    }
}
