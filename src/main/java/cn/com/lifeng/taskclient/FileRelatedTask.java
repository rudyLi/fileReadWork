package cn.com.lifeng.taskclient;

import java.nio.ByteBuffer;

/**
 * Created by lifeng on 16/3/27.
 */
public interface FileRelatedTask {
    public String start();
    public void setBuffer(ByteBuffer[] buffer);
}
