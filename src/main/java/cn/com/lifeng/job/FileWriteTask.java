package cn.com.lifeng.job;

import cn.com.lifeng.util.CommonConstant;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileWriteTask implements FileRelatedTask {
    private String inputFilePath;
    private String outputFilePath;
    private long beginLineNumber;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

    public FileWriteTask(String inputFilePath, String outPutFilePath, ByteBuffer readBuffer, ByteBuffer writeBuffer, long beginLineNumber) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outPutFilePath;
        this.beginLineNumber = beginLineNumber;
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
    }

    public int start() {
        int result = 0;
        long lineNumber = beginLineNumber;
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
                int loc = 0;
                for (int i = 0; readBuffer.hasRemaining(); i++) {
                    if (readBuffer.get() == CommonConstant.LINE_SEPARATOR[0]) loc += 1;
                }
                //重新读
                readBuffer.rewind();
                for (int lineTotal = 0; lineTotal < loc; lineTotal++) {
                    //input line number and dot
                    // todo putLong putChar会补充多余的字节，所以只想到了这个方法
                    writeBuffer.put((Long.toString(lineNumber)).getBytes());
                    writeBuffer.put(CommonConstant.DOT);
                    for (int i = 0; readBuffer.hasRemaining(); i++) {
                        byte tmp = readBuffer.get();
                        writeBuffer.put(tmp);
                        if (tmp == CommonConstant.LINE_SEPARATOR[0]) {
                            // 换行时 写数据，重新clear
                            writeBuffer.flip();
                            writeChannel.write(writeBuffer);
                            writeBuffer.clear();
                            lineNumber++;
                            break;
                        }
                    }
                }
                //读数据时并没完全读完
                readBuffer.compact();
            }
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
                if (result > 0)
                    System.out.println("input:" + inputFilePath + "---------" + "lineNumber:" + beginLineNumber);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public ByteBuffer[] release() {
        ByteBuffer[] result = new ByteBuffer[2];
        result[0] = readBuffer;
        result[1] = writeBuffer;
        return result;
    }

}
