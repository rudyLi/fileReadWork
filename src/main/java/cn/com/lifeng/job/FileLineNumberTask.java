package cn.com.lifeng.job;

import cn.com.lifeng.util.CommonConstant;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileLineNumberTask implements FileRelatedTask {
    private String filePath;
    private ByteBuffer byteBuffer;

    public FileLineNumberTask(String filePath, ByteBuffer byteBuffer) {
        this.filePath = filePath;
        this.byteBuffer = byteBuffer;
    }

    // -1代表文件读取出现问题
    public int start() {
        byteBuffer.clear();
        int lineNumber = 0;
        //同时出现换行符才会计数,windows换行符多个字节
        State transferState = new State();
        FileInputStream fileInputStream = null;
        FileChannel fileChannel = null;
        try {
            fileInputStream = new FileInputStream(filePath);
            fileChannel = fileInputStream.getChannel();
            while (true) {
                int eof = fileChannel.read(byteBuffer);
                if (eof == -1) break;
                byteBuffer.flip();
                for (int i = 0; byteBuffer.hasRemaining(); i++) {
                    byte tmp = byteBuffer.get();
                    if(transferState.doTransfer(tmp)==transferState.size) {
                        lineNumber += 1;
                    }
                }
                byteBuffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
            lineNumber = -1;
        } finally {
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return lineNumber;
    }

    @Override
    public ByteBuffer[] release() {
        ByteBuffer[] a = new ByteBuffer[1];
        a[0] = byteBuffer;
        return a;
    }

    //状态转移 当 行分隔符为 '\r\n',则状态转移有三种状态（0，1，2） 起始状态为0，每次拿当前状态的对比，如果满足则状态加一，否则减一，连续两个匹配上则状态到达完美匹配状态2
    private class State {
        int currentState = 0;
        int size = CommonConstant.LINE_SEPARATOR.length;
        byte[] transfer = CommonConstant.LINE_SEPARATOR;

        int doTransfer(byte tmp) {
            if (currentState == size) currentState = 0;
            if (transfer[currentState] == tmp) {
                currentState++;
            } else if (currentState > 0) {
                currentState--;
            }
            return currentState;
        }
    }
}
