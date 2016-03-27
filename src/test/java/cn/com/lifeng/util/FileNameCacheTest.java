package cn.com.lifeng.util;

import cn.com.lifeng.bootstrap.FileNameCache;
import junit.framework.TestCase;


/**
 * Created by lifeng on 16/3/27.
 */
public class FileNameCacheTest extends TestCase {

    public void testAddFile() throws Exception {
        FileNameCache fileNameCache = new FileNameCache("/home","/home/output","/home/logtest.2010-01-01.log");
        fileNameCache.addFile("/home/logtest.2013-01-01.log");
        fileNameCache.addFile("/home/la");
        fileNameCache.addFile("/home/logtest.2012-01-01.log");
        fileNameCache.addFile("/home/logtest.2014-01-01.log");
        fileNameCache.addFile("/home/logtest.2013-02-01.log");
        assertEquals(4, fileNameCache.getSize());
        assertEquals("logtest.2012-01-01.log", fileNameCache.getFileNameByIndex(0));
        assertEquals("/home/logtest.2013-01-01.log", fileNameCache.getInputFileNameByIndex(1));
        assertEquals("/home/output/logtest.2013-02-01.log", fileNameCache.getOutputFileNameByIndex(2));
        assertEquals("/home/output/logtest.2014-01-01.log", fileNameCache.getOutputFileNameByIndex(3));
        fileNameCache.addFile("/home/logtest.2009-01-01.log");
        assertEquals(4, fileNameCache.getSize());
        System.out.println("\r\n".getBytes().length);
    }
}