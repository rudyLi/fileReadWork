package cn.com.lifeng;

import cn.com.lifeng.bootstrap.Scheduler;
import cn.com.lifeng.bootstrap.FileNameCache;
import cn.com.lifeng.job.JobStatusCacheTask;
import cn.com.lifeng.util.FileUtil;
import cn.com.lifeng.util.JobStatus;
import org.apache.commons.cli.*;

import java.io.File;

/**
 * Hello world!
 */
public class Bootstrap {

    private int threadNum = 10;
    private String statusFilePath = "./status";
    private String inputPath;
    private String outputPath;
    private String currentFileName = null;
    private long  currentLineNumber = 1;
    private Scheduler scheduler;
    public Bootstrap(String inputPath, String outputPath) throws Exception {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        FileUtil.isAbsolute(inputPath);
        FileUtil.isAbsolute(inputPath);
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public void setTaskStatusPath( String taskStatusFilePath) throws Exception {
        statusFilePath = taskStatusFilePath;
        FileUtil.isAbsolute(statusFilePath);
    }

    public void setStartJobStatus(String startFileName, String startColumnNum) {
        this.currentFileName = startFileName;
        this.currentLineNumber = Long.parseLong(startColumnNum);
    }

    private void initial() throws Exception {
        //检查相应的目录是否创建成果
        checkFileExist();
        // 初始化jobstatus
        JobStatus jobStatus = JobStatusCacheTask.getTaskStatus(statusFilePath);
        // 修改 job运行状态
        if(currentFileName!=null){
            jobStatus.setCurrentFileName(currentFileName);
            jobStatus.setCurrentLineNumber(currentLineNumber);
        }
        FileNameCache fileNameCache = new FileNameCache(inputPath, outputPath, jobStatus.getCurrentFileName());
        scheduler = new Scheduler(fileNameCache, jobStatus);
        scheduler.setThreadNum(threadNum);
    }

    private void checkFileExist() throws Exception {
        FileUtil.checkFileExist(inputPath);
        FileUtil.mkDir(outputPath);
        FileUtil.mkDir(statusFilePath.substring(0, statusFilePath.lastIndexOf(File.separator)));
    }

    public void start() throws Exception {
        initial();
        scheduler.start();
    }

    public static void main(String[] args) {
        String inputFileCommand = "inputPath";
        String outputFileCommand = "outputPath";
        String threadNumCommand = "threadNum";
        String startFilePathCommand = "startFileName";
        String startColumnNumCommand = "startColumnNum";
        String taskStatusFilePathCommand = "taskStatusPath";

        Options options = new Options();
        options.addOption(inputFileCommand, true, "Must. EG: /input,WARN: Must ensure input and output can not the same directory，please use absolute path");
        options.addOption(outputFileCommand, true, "Must. EG: /output,WARN: Must ensure input and output can not the same directory, please use absolute path");
        options.addOption(threadNumCommand, false, "Option. Default is 10,you can define the var,EG:20");
        options.addOption(startFilePathCommand, false, "Option.This command must be used with startFilePath,startColumnNum together");
        options.addOption(startColumnNumCommand, false, "Option. This command must be used with startFilePath,startColumnNum together");
        options.addOption(taskStatusFilePathCommand, false, "Option. please use absolute path");

        CommandLineParser parser = new GnuParser();
        HelpFormatter helper = new HelpFormatter();

        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            if (!line.hasOption(inputFileCommand) || !line.hasOption(outputFileCommand)) {
                helper.printHelp("Read File : ", options);
                System.exit(-1);
            }
            if (line.getOptionValue(inputFileCommand).equals(line.getOptionValue(outputFileCommand))) {
                helper.printHelp("Read File : ", options);
                System.exit(-1);
            }
            Bootstrap bootstrap = new Bootstrap(line.getOptionValue(inputFileCommand), line.getOptionValue(outputFileCommand));
            if (line.hasOption(threadNumCommand)) {
                bootstrap.setThreadNum(Integer.parseInt(line.getOptionValue(threadNumCommand)));
            }
            if (line.hasOption(startColumnNumCommand) && line.hasOption(startFilePathCommand)) {
                bootstrap.setStartJobStatus(line.getOptionValue(startFilePathCommand), line.getOptionValue(startColumnNumCommand));
            }
            if (line.hasOption(taskStatusFilePathCommand)) {
                bootstrap.setTaskStatusPath(line.getOptionValue(taskStatusFilePathCommand));
            }
            bootstrap.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
            helper.printHelp("Read File: ", options);
        }
    }
}
