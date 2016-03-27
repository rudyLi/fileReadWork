package cn.com.lifeng.taskclient;

import cn.com.lifeng.util.CommonConstant;
import cn.com.lifeng.util.LineJudgeState;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileWriteTask implements FileRelatedTask {
    static Logger logger = Logger.getLogger(FileWriteTask.class.getName());

    private String inputFilePath;
    private String outputFilePath;
    private long beginLineNumber;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

    public FileWriteTask(String inputFilePath, String outPutFilePath, long beginLineNumber) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outPutFilePath;
        this.beginLineNumber = beginLineNumber;
    }

    public String start() {
        int result = 0;
        long lineNumber = beginLineNumber;
        LineJudgeState transferState = new LineJudgeState(CommonConstant.LINE_SEPARATOR.length);
        readBuffer.clear();
        writeBuffer.clear();
        FileInputStream fileInputStream = null;
        FileChannel readChannel = null;
        FileOutputStream fileOutputStream = null;
        FileChannel writeChannel = null;
        try {
            fileInputStream = new FileInputStream(inputFilePath);
            readChannel = fileInputStream.getChannel();
            fileOutputStream = new FileOutputStream(outputFilePath);
            writeChannel = fileOutputStream.getChannel();
            while (true) {
                int eof = readChannel.read(readBuffer);
                if (eof == -1) break;
                readBuffer.flip();
                for (int i = 0; readBuffer.hasRemaining(); i++) {
                    // 首先判断当前状态，当前的操作要根据上一次转移的状态确定
                    if(transferState.getCurrentState()==transferState.getPerfectState()){
                        write(writeChannel, Long.toString(lineNumber).getBytes());
                        write(writeChannel, CommonConstant.DOT);
                        lineNumber+=1;
                    }
                    byte tmp = readBuffer.get();
                    transferState.doTransfer(tmp);
                    write(writeChannel, tmp);
                }
                //全部数据清除
                readBuffer.clear();
            }
            //最后写一下数据
            writeBuffer.flip();
            writeChannel.write(writeBuffer);
            writeBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
            result = -1;
        } finally {
            try {
                if (readChannel != null) {
                    readChannel.close();
                }
                if (writeChannel != null) {
                    writeChannel.close();
                }
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
                if (result >= 0)
                    logger.info("input:" + inputFilePath + "---------" + "lineNumber:" + beginLineNumber);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Integer.toString(result);
    }

    @Override
    public void setBuffer(ByteBuffer[] buffer) {
        readBuffer = buffer[0];
        writeBuffer = buffer[1];
    }

    private void write(FileChannel fileChannel, byte... array) throws IOException {
       for(byte ele: array){
           if(!writeBuffer.hasRemaining()) {
               writeBuffer.flip();
               fileChannel.write(writeBuffer);
               writeBuffer.clear();
           }
           writeBuffer.put(ele);
           fileChannel.force(false);
       }
    }

}
