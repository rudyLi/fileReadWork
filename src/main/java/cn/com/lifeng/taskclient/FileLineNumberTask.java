package cn.com.lifeng.taskclient;

import cn.com.lifeng.util.LineJudgeState;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileLineNumberTask implements FileRelatedTask {
    private int index;
    private String filePath;
    private ByteBuffer byteBuffer;

    public FileLineNumberTask(String filePath, int index) {
        this.filePath = filePath;
        this.index = index;
    }
    // -1代表文件读取出现问题
    public String start() {
        byteBuffer.clear();
        int lineNumber = 0;
        //同时出现换行符才会计数,windows换行符多个字节
        LineJudgeState transferState = new LineJudgeState(0);
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
                    if(transferState.doTransfer(tmp)==transferState.getPerfectState()) {
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
       return constructResult(lineNumber);
    }

    private String constructResult(int lineNumber){
        String result;
        if(lineNumber>=0){
            result = index+"-"+lineNumber;
        }else {
            result = "-1";
        }
        return result;
    }

    @Override
    public void setBuffer(ByteBuffer[] buffer) {
        byteBuffer = buffer[0];
    }


}
