package cn.com.lifeng.util;

/**
 * Created by lifeng on 16/3/26.
 */
public class JobStatus {
    private volatile double currentLineNumber=0;
    private volatile String currentFileName =null;
    private String statusFilePath;

    public double getCurrentLineNumber() {
        return currentLineNumber;
    }

    public void setCurrentLineNumber(double currentLineNumber) {
        this.currentLineNumber = currentLineNumber;
    }

    public String getCurrentFileName() {
        return currentFileName;
    }

    public void setCurrentFileName(String currentFileName) {
        this.currentFileName = currentFileName;
    }

    public String getStatusFilePath() {
        return statusFilePath;
    }

    public void setStatusFilePath(String statusFilePath) {
        this.statusFilePath = statusFilePath;
    }
}
