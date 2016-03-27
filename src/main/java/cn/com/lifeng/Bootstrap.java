package cn.com.lifeng;

import cn.com.lifeng.bootstrap.Scheduler;
import cn.com.lifeng.util.FileNameUtil;
import cn.com.lifeng.bootstrap.JobStatusCacheTask;
import cn.com.lifeng.util.JobStatus;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Hello world!
 */
public class Bootstrap {
    private FileNameUtil fileNameUtil;
    private JobStatus jobStatus;
    private Scheduler scheduler;
    private int threadNum = 10;
    private String statusFilePath = "./status";

    public Bootstrap(String inputPath, String outputPath) throws Exception {
        checkFileExist(inputPath, outputPath);
        fileNameUtil = new FileNameUtil(inputPath, outputPath);
        jobStatus = JobStatusCacheTask.getTaskStatus(statusFilePath);
    }

    private void checkFileExist(String inputPath, String outputPath) throws Exception {
        FileNameUtil.checkFileExist(inputPath);
        FileNameUtil.mkDir(outputPath);
        checkStatusPath(statusFilePath);
    }

    private void checkStatusPath(String path) throws Exception {
        FileNameUtil.mkDir(path.substring(0, path.lastIndexOf(File.separator)));
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public void setStartJobStatus(String startFileName, String startColumnNum, String taskStatusFilePath) throws Exception {
        checkStatusPath(taskStatusFilePath);
        setStartJobStatus(startFileName,startColumnNum);
        jobStatus.setStatusFilePath(taskStatusFilePath);
    }

    public void setStartJobStatus(String startFileName, String startColumnNum) {
        jobStatus.setCurrentFileName(startFileName);
        jobStatus.setCurrentLineNumber(Long.parseLong(startColumnNum));
    }

    public void start() {
        fileNameUtil.setFileBegin(jobStatus.getCurrentFileName());
        fileNameUtil.startListAllFile();
        scheduler = new Scheduler(fileNameUtil, jobStatus);
        scheduler.setThreadNum(threadNum);
        scheduler.start();
    }

    public static void main(String[] args) {
        //参数  inputPath outPutPath threadNum startFileName startColumnNum taskStatusFilePath
        String inputFileCommand = "inputPath";
        String outputFileCommand = "outputPath";
        String threadNumCommand = "threadNum";
        String startFilePathCommand = "startFilePath";
        String startColumnNumCommand = "startColumnNum";
        String taskStatusFilePathCommand = "taskStatusFile";

        Options options = new Options();
        options.addOption(inputFileCommand, true, "Must. EG: ./input,WARN: Must ensure input and output can not the same directory，please use absolute path");
        options.addOption(outputFileCommand, true, "Must. EG: ./output,WARN: Must ensure input and output can not the same directory, please use absolute path");
        options.addOption(threadNumCommand, false, "Option. Default is 10,you can define the var,EG:20");
        options.addOption(startFilePathCommand, false, "Option.This command must be used with startFilePath,startColumnNum together");
        options.addOption(startColumnNumCommand, false, "Option. This command must be used with startFilePath,startColumnNum together");
        options.addOption(taskStatusFilePathCommand, false, "Option. This command must be used with startFilePath,startColumnNum,taskStatusFile together");

        CommandLineParser parser = new GnuParser();
        HelpFormatter helper = new HelpFormatter();

        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            if (!line.hasOption(inputFileCommand) || !line.hasOption(outputFileCommand)) {
                helper.printHelp("Read File : ", options);
                System.exit(0);
            }
            if (line.getOptionValue(inputFileCommand).equals(line.getOptionValue(outputFileCommand))) {
                helper.printHelp("Read File : ", options);
                System.exit(0);
            }
            Bootstrap bootstrap = new Bootstrap(line.getOptionValue(inputFileCommand), line.getOptionValue(outputFileCommand));
            if (line.hasOption(threadNumCommand)) {
                bootstrap.setThreadNum(Integer.parseInt(line.getOptionValue(threadNumCommand)));
            }
            if (line.hasOption(startColumnNumCommand) && line.hasOption(startFilePathCommand)) {
                if (line.hasOption(taskStatusFilePathCommand)) {
                    bootstrap.setStartJobStatus(line.getOptionValue(startFilePathCommand), line.getOptionValue(startColumnNumCommand), line.getOptionValue(taskStatusFilePathCommand));
                } else {
                    bootstrap.setStartJobStatus(line.getOptionValue(startFilePathCommand), line.getOptionValue(startColumnNumCommand));
                }
            }
            bootstrap.start();
        } catch (Exception e) {
            e.printStackTrace();
            helper.printHelp("Read File: ", options);
        }
    }
}
