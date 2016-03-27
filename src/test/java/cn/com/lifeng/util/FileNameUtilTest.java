package cn.com.lifeng.util;

import junit.framework.TestCase;

import java.io.File;


/**
 * Created by lifeng on 16/3/27.
 */
public class FileNameUtilTest extends TestCase {

    public void testAddFile() throws Exception {
        FileNameUtil fileNameUtil = new FileNameUtil("/home","/home/output");
        fileNameUtil.addFile("/home/logtest.2013-01-01.log");
        fileNameUtil.addFile("/home/la");
        fileNameUtil.addFile("/home/logtest.2012-01-01.log");
        fileNameUtil.addFile("/home/logtest.2014-01-01.log");
        fileNameUtil.addFile("/home/logtest.2013-02-01.log");
        assertEquals(4, fileNameUtil.getSize());
        assertEquals("logtest.2012-01-01.log", fileNameUtil.getFileNameByIndex(0));
        assertEquals("/home/logtest.2013-01-01.log",fileNameUtil.getInputFileNameByIndex(1));
        assertEquals("/home/output/logtest.2013-02-01.log",fileNameUtil.getOutputFileNameByIndex(2));
        assertEquals("/home/output/logtest.2014-01-01.log", fileNameUtil.getOutputFileNameByIndex(3));
        long i = 10;
        System.out.println(i);
    }
}