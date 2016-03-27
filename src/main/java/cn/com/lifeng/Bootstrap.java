package cn.com.lifeng;

import cn.com.lifeng.taskserver.Scheduler;
import cn.com.lifeng.taskclient.FileNameCache;
import cn.com.lifeng.taskserver.JobStatusCacheTask;
import cn.com.lifeng.taskclient.TaskSubmitter;
import cn.com.lifeng.util.FileUtil;
import cn.com.lifeng.util.JobStatus;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import java.io.File;

public class Bootstrap {
    static Logger logger = Logger.getLogger(Bootstrap.class.getName());

    private int threadNum = 10;
    private String statusFilePath=null;
    private String inputPath;
    private String outputPath;
    private String currentFileName = null;
    private long currentLineNumber = 1;
    private int bufferSize = 1024*1024;
    private TaskSubmitter taskSubmitter;

    public Bootstrap(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public void setTaskStatusPath(String taskStatusFilePath) throws Exception {
        statusFilePath = taskStatusFilePath;
        FileUtil.isAbsolute(statusFilePath);
    }

    public void setStartJobStatus(String startFileName, String startColumnNum) {
        this.currentFileName = startFileName;
        this.currentLineNumber = Long.parseLong(startColumnNum);
    }

    private void initialRelatedFile(){
        if(!FileUtil.isAbsolute(inputPath) || !FileUtil.isAbsolute(inputPath)
                || !FileUtil.checkFileExist(inputPath) || !FileUtil.mkDir(outputPath)) {
            logger.error("The inputPath or outputPath has something wrong,please check it");
            System.exit(-1);
        }
        if (statusFilePath==null) statusFilePath = System.getProperty("user.dir")+File.separator + "status";
        //取出其中的目录路径
        if(!FileUtil.mkDir(new File(statusFilePath).getParent())){
            logger.error("The statusFilePath has something wrong,please check it");
            System.exit(-1);
        }
    }

    private void initial() throws Exception {
        //检查相应的目录是否创建成果
        initialRelatedFile();
        // 初始化jobstatus
        JobStatus jobStatus = JobStatusCacheTask.getTaskStatus(statusFilePath);
        // 修改 job运行状态
        if (currentFileName != null) {
            jobStatus.setCurrentFileName(currentFileName);
            jobStatus.setCurrentLineNumber(currentLineNumber);
        }
        FileNameCache fileNameCache = new FileNameCache(inputPath, outputPath, jobStatus.getCurrentFileName());
        Scheduler scheduler = new Scheduler(threadNum, bufferSize);
        taskSubmitter = new TaskSubmitter(fileNameCache,jobStatus);
        taskSubmitter.setScheduler(scheduler);
    }

    public void start() throws Exception {
        initial();
        taskSubmitter.start();
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
        options.addOption(threadNumCommand, true, "Option. Default is 10,you can define the var,EG:20");
        options.addOption(startFilePathCommand, true, "Option.This command must be used with startFilePath,startColumnNum together");
        options.addOption(startColumnNumCommand, true, "Option. This command must be used with startFilePath,startColumnNum together");
        options.addOption(taskStatusFilePathCommand, true, "Option. please use absolute path");

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
            helper.printHelp("Read File: ", options);
        }
    }
}
