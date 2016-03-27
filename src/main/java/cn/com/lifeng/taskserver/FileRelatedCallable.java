package cn.com.lifeng.taskserver;

import cn.com.lifeng.taskclient.FileRelatedTask;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 * Created by lifeng on 16/3/27.
 */
public class FileRelatedCallable implements Callable<String> {
    private ResourceManager resourceManager;
    private FileRelatedTask delegate;
    private ByteBuffer[] byteBuffers;

    public FileRelatedCallable(FileRelatedTask fileRelatedTask, ResourceManager resourceManager, ByteBuffer[] byteBuffers) {
        delegate = fileRelatedTask;
        this.resourceManager = resourceManager;
        this.byteBuffers = byteBuffers;
    }

    @Override
    public String call() {
        delegate.setBuffer(byteBuffers);
        String result = delegate.start();
        try {
            resourceManager.put(byteBuffers);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }
}
