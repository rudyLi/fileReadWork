package cn.com.lifeng.job;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileWriteTask implements FileRelatedTask {
    private String inputFilePath;
    private String outputFilePath;
    private long beginLineNumber;

    public FileWriteTask(String inputFilePath, String outPutFilePath, long beginLineNumber) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outPutFilePath;
        this.beginLineNumber = beginLineNumber;
    }

    public int start() {
        int result = 0;
        long lineNumber = beginLineNumber;
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
            while ((tmp = bufferedReader.readLine()) != null) {
                bufferedWriter.write(lineNumber + "." + tmp);
                bufferedWriter.newLine();
                lineNumber += 1;
            }
        } catch (IOException e) {
            e.printStackTrace();
            result = -1;
        } finally {
            try {
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
                if(result>0) System.out.println("input:" + inputFilePath + "---------" + "lineNumber:" + beginLineNumber);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
