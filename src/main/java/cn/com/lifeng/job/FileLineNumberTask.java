package cn.com.lifeng.job;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileLineNumberTask implements FileRelatedTask {
    private String filePath;

    public FileLineNumberTask(String filePath) {
        this.filePath = filePath;
    }
    // -1代表文件读取出现问题
    public int start() {
        int lineNumber = 0;
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileReader = new FileReader(filePath);
            //buffer size 50k byte
            bufferedReader = new BufferedReader(fileReader, 51200);
            while (bufferedReader.readLine() != null) {
                lineNumber += 1;
            }
        } catch (IOException e) {
            e.printStackTrace();
            lineNumber = -1;
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return lineNumber;
    }
}
