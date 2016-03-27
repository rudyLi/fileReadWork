package cn.com.lifeng.job;

import java.nio.ByteBuffer;

/**
 * Created by lifeng on 16/3/27.
 */
public interface FileRelatedTask {
    public int start();

    public ByteBuffer[] release();
}
