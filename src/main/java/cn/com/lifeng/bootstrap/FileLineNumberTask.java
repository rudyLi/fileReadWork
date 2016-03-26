package cn.com.lifeng.bootstrap;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileLineNumberTask {
    private String filePath;
    private int retryTimes;
    public FileLineNumberTask(String filePath){
        this(filePath,3);
    }
    public FileLineNumberTask(String filePath,int retryTimes){
        this.filePath = filePath;
        this.retryTimes = retryTimes;
    }
    // -1代表文件读取出现问题
    public int getLineNumber(){
        int lineNumber = 0;
        boolean readIsSuccess = true;
        while (retryTimes>0) {
            //每次运行时需初始化
            lineNumber = 0;
            readIsSuccess = true;

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
                readIsSuccess = false;
            } finally {
                try {
                    retryTimes -= 1;
                    if(bufferedReader!=null){
                        bufferedReader.close();
                    }
                    if(fileReader!=null){
                        fileReader.close();
                    }
                    //最后一次时不需要sleep
                    if(retryTimes>0) TimeUnit.MILLISECONDS.sleep(10);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return readIsSuccess ? lineNumber:-1;
    }
}
