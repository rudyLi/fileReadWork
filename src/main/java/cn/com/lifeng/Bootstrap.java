package cn.com.lifeng;

import cn.com.lifeng.bootstrap.Scheduler;
import cn.com.lifeng.util.FileNameUtil;
import cn.com.lifeng.bootstrap.JobStatusCacheTask;
import cn.com.lifeng.util.JobStatus;
import org.apache.commons.cli.*;

/**
 * Hello world!
 *
 */
public class Bootstrap
{
    private FileNameUtil fileNameUtil;
    private JobStatus jobStatus;
    private Scheduler scheduler;
    private int threadNum = 10;
    private String statusFilePath = "./status";
    public Bootstrap(String inputPath,String outputPath){
        fileNameUtil = new FileNameUtil(inputPath,outputPath);
        jobStatus = JobStatusCacheTask.getTaskStatus(statusFilePath);
    }
    public void setThreadNum(int threadNum){
        this.threadNum = threadNum;
    }
    public void setStartJobStatus(String startFileName,String startColumnNum,String taskStatusFilePath){
        jobStatus.setCurrentFileName(startFileName);
        jobStatus.setCurrentLineNumber(Double.parseDouble(startColumnNum));
        jobStatus.setStatusFilePath(taskStatusFilePath);
    }

    public void start(){
        fileNameUtil.setFileBegin(jobStatus.getCurrentFileName());
        fileNameUtil.startListAllFile();
        scheduler = new Scheduler(fileNameUtil, jobStatus);
        scheduler.setThreadNum(threadNum);
        scheduler.start();
    }
    public void stop(){
        scheduler.stop();
    }
    public static void main( String[] args ){
        //参数  inputPath outPutPath threadNum startFileName startColumnNum taskStatusFilePath
        String inputFileCommand = "inputPath";
        String outputFileCommand = "outputPath";
        String threadNumCommand = "threadNum";
        String startFilePathCommand = "startFilePath";
        String startColumnNumCommand = "startColumnNum";
        String taskStatusFilePathCommand = "taskStatusFile";
        Options options = new Options();
        options.addOption(inputFileCommand, true, "EG: ./input");
        options.addOption(outputFileCommand, true, "EG: ./output");
        options.addOption(threadNumCommand, false, "Default is 10,you can define the var,EG:20");
        options.addOption(startFilePathCommand, false, "This command must be used with startFilePath,startColumnNum,taskStatusFile together");
        options.addOption(startColumnNumCommand, false, "This command must be used with startFilePath,startColumnNum,taskStatusFile together");
        options.addOption(taskStatusFilePathCommand, false, "This command must be used with startFilePath,startColumnNum,taskStatusFile together");

        CommandLineParser parser = new GnuParser();
        HelpFormatter helper = new HelpFormatter();

        CommandLine line =null;
        try {
            line=parser.parse(options,args);
            if(!line.hasOption(inputFileCommand) || !line.hasOption(outputFileCommand)){
                helper.printHelp("Read File : ", options);
            }
            Bootstrap bootstrap = new Bootstrap(line.getOptionValue(inputFileCommand),line.getOptionValue(outputFileCommand));
            if(line.hasOption(threadNumCommand)){
                bootstrap.setThreadNum(Integer.parseInt(line.getOptionValue(threadNumCommand)));
            }
            if(line.hasOption(startColumnNumCommand)&&line.hasOption(startFilePathCommand)&&line.hasOption(taskStatusFilePathCommand)){
                bootstrap.setStartJobStatus(line.getOptionValue(startFilePathCommand),line.getOptionValue(startColumnNumCommand),line.getOptionValue(taskStatusFilePathCommand));
            }
            bootstrap.start();
        } catch (ParseException e) {
            e.printStackTrace();
            helper.printHelp("Read File: ", options);
        }
    }
}
