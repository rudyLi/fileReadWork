package cn.com.lifeng.bootstrap;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileWriteTask {
    private String inputFilePath;
    private String outputFilePath;
    private double beginLineNumber;
    private int retryTimes;
    public FileWriteTask(String inputFilePath,String outPutFilePath,double beginLineNumber){
        this(inputFilePath,outPutFilePath,beginLineNumber,3);
    }
    public FileWriteTask(String inputFilePath,String outPutFilePath,double beginLineNumber, int retryTimes){
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outPutFilePath;
        this.beginLineNumber = beginLineNumber;
        this.retryTimes = retryTimes;
    }
    public boolean startWrite() {
        boolean writeIsSuccess = true;
        while (retryTimes > 0) {
            double lineNumber = beginLineNumber;
            writeIsSuccess = true;
            FileReader fileReader = null;
            BufferedReader bufferedReader = null;
            FileWriter fileWriter = null;
            BufferedWriter bufferedWriter = null;
            try {
                fileReader = new FileReader(inputFilePath);
                //buffer size 50k byte
                bufferedReader = new BufferedReader(fileReader, 51200);
                //初始化其字节大小为30字节，默认缓存大小8k字节
                fileWriter = new FileWriter(outputFilePath);
                //buffer size 50k byte
                bufferedWriter = new BufferedWriter(fileWriter, 51200);
                String tmp;
                while ((tmp=bufferedReader.readLine())!=null){
                    bufferedWriter.write(lineNumber+"."+tmp);
                    bufferedWriter.newLine();
                    lineNumber += 1;
                }
            } catch (IOException e) {
                e.printStackTrace();
                writeIsSuccess = false;
            } finally {
                try {
                    retryTimes -= 1;
                    if (bufferedWriter != null) {
                        bufferedWriter.close();
                    }
                    if (fileWriter != null) {
                        fileWriter.close();
                    }
                    if (bufferedReader != null) {
                        bufferedReader.close();
                    }
                    if (fileReader != null) {
                        fileReader.close();
                    }
                    if(retryTimes>0) TimeUnit.MILLISECONDS.sleep(10);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return writeIsSuccess;
    }
}
