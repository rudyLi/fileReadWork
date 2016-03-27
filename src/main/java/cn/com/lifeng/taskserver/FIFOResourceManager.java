package cn.com.lifeng.taskserver;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by lifeng on 16/3/28.
 */
public class FIFOResourceManager implements ResourceManager {
    private LinkedBlockingQueue<ByteBuffer> resource;
    private int bufferSize;
    private int capacity;
    public FIFOResourceManager(int capacity,int bufferSize){
        this.bufferSize = bufferSize;
        this.capacity = capacity;
        init();
    }

    public void init(){
        resource = new LinkedBlockingQueue<ByteBuffer>(capacity);
        for (int i = 0; i < capacity; i++) {
            resource.offer(ByteBuffer.allocateDirect(bufferSize));
        }
    }
    @Override
    public ByteBuffer get() throws InterruptedException {
        return resource.take();
    }

    @Override
    public void put(ByteBuffer... byteBuffers) throws InterruptedException {
        for(ByteBuffer byteBuffer:byteBuffers){
            resource.put(byteBuffer);
        }
    }

    @Override
    public void release() {
        for(ByteBuffer byteBuffer:resource){
            byteBuffer=null;
        }
    }
}
