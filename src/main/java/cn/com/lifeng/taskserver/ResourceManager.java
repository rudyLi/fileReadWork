package cn.com.lifeng.taskserver;

import java.nio.ByteBuffer;

/**
 * Created by lifeng on 16/3/28.
 */
public interface ResourceManager {
    public ByteBuffer get() throws InterruptedException;
    public void put(ByteBuffer... byteBuffers) throws InterruptedException;
    public void release();
}
