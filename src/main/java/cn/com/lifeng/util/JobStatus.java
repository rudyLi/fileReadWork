package cn.com.lifeng.util;

/**
 * Created by lifeng on 16/3/26.
 */
public class JobStatus {
    private long currentLineNumber = 1;
    private String currentFileName = null;
    private String statusFilePath;

    public static JobStatus clone(JobStatus jobStatus) {
        JobStatus newJobStatus = new JobStatus();
        newJobStatus.setCurrentFileName(jobStatus.getCurrentFileName());
        newJobStatus.setCurrentLineNumber(jobStatus.getCurrentLineNumber());
        newJobStatus.setStatusFilePath(jobStatus.getStatusFilePath());
        return newJobStatus;
    }

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

    //todo 处理job status状态读取格式.以冒号分隔，文件中有冒号会发生错误
    public void setFileNameAndLineNumber(String fileNameAndLineNumber) {
        if (fileNameAndLineNumber != null && !fileNameAndLineNumber.trim().isEmpty()) {
            String[] taskStatus = fileNameAndLineNumber.split(":");
            setCurrentFileName(taskStatus[0]);
            setCurrentLineNumber(Long.parseLong(taskStatus[1]));
        }
    }

    public String getFileNameAndLineNumber() {
        return this.currentFileName + ":" + this.currentLineNumber;
    }

    public String getStatusFilePath() {
        return statusFilePath;
    }

    public void setStatusFilePath(String statusFilePath) {
        this.statusFilePath = statusFilePath;
    }

    public boolean hasInitialed(){
        if(currentFileName==null){
            return false;
        }
        return true;
    }
    public String toString(){
        if(currentFileName==null){
            return "";
        }
        return currentFileName+":"+currentLineNumber;
    }
}
