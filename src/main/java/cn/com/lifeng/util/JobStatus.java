package cn.com.lifeng.util;

/**
 * Created by lifeng on 16/3/26.
 */
public class JobStatus {
    private long currentLineNumber = 1;
    private String currentFileName = null;
    private String statusFilePath;

    public long getCurrentLineNumber() {
        return currentLineNumber;
    }

    public void setCurrentLineNumber(long currentLineNumber) {
        this.currentLineNumber = currentLineNumber;
    }

    public String getCurrentFileName() {
        return currentFileName;
    }

    public void setCurrentFileName(String currentFileName) {
        this.currentFileName = currentFileName;
    }

    public synchronized void setFileNameAndLineNumber(String currentFileName, long currentLineNumber) {
        this.currentFileName = currentFileName;
        this.currentLineNumber = currentLineNumber;
    }

    //todo 处理job status状态读取格式.以冒号分隔，文件中有冒号会发生错误
    public void setFileNameAndLineNumber(String fileNameAndLineNumber) {
        if(fileNameAndLineNumber!=null && !fileNameAndLineNumber.trim().isEmpty()) {
            String[] taskStatus = fileNameAndLineNumber.split(":");
            setCurrentFileName(taskStatus[0]);
            setCurrentLineNumber(Long.parseLong(taskStatus[1]));
        }
    }

    public synchronized String getFileNameAndLineNumber() {
        return this.currentFileName + ":" + this.currentLineNumber;
    }

    public String getStatusFilePath() {
        return statusFilePath;
    }

    public void setStatusFilePath(String statusFilePath) {
        this.statusFilePath = statusFilePath;
    }
}
