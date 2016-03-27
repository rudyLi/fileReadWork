package cn.com.lifeng.bootstrap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by lifeng on 16/3/26.
 */
public class FileNameCache {
    /*1000000个小文件，如果直接记录名字的话，比如filename是'logtest.2014-06-05.log',一个filename占用存储空间为22个字节,
    则光名字存储就需要22M，所以对filename进行简化，只需要存储其日期数字即可，这样一个filename存储空间为4个字节，占用内存接近
    3M，至于不使用jdk自带的数据结构，是为了减少内存的使用
     */
    private int capacity = 1000000;
    private int size = 0;
    private int[] fileNameCache = new int[capacity];
    private int fileDateBegin = 0;
    private final static String FILE_PREFIX = "logtest.";
    private final static String FILE_POSTFIX = ".log";
    private final static int FILE_LENGTH = 22;
    private SimpleDateFormat displayFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat storageFormat = new SimpleDateFormat("yyyyMMdd");
    private String inputPath;
    private String outputPath;

    public FileNameCache(String inputPath, String outputPath, String fileBegin) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;

        processFilePath(fileBegin);
    }

    private void processFilePath(String fileBegin) {
        if (!inputPath.endsWith(File.separator)) {
            inputPath += File.separator;
        }
        if (!outputPath.endsWith(File.separator)) {
            outputPath += File.separator;
        }
        if (fileBegin != null) {
            this.fileDateBegin = parseFileDate(fileBegin);
        }
    }

    public void startListAllFile() {
        Path dir = Paths.get(inputPath);
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
            for (Path file : stream) {
                addFile(file.getFileName().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getSize() {
        return size;
    }

    //thread unsafe 日期解析非线程安全的
    private int parseFileDate(String fileName) {
        //todo 拿到具体文件名,支持相对路径时需做修改
        fileName = fileName.replace(inputPath, "");
        int date = 0;
        // 判断file合法性，
        if (fileName != null && fileName.length() == FILE_LENGTH && fileName.startsWith(FILE_PREFIX) && fileName.endsWith(FILE_POSTFIX)) {
            String tmp = fileName.split("\\.")[1];
            try {
                Date tmpDate = displayFormat.parse(tmp);
                date = Integer.parseInt(storageFormat.format(tmpDate));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return date;
    }

    public void addFile(String fileName) {
        int date = parseFileDate(fileName);
        if (date > fileDateBegin) {
            ensureCapacity(size + 1);
            sortAndAddFileName(date);
            size += 1;
        }
    }

    // 二分查找 排序已有数组为数组从小到大的排序
    private void sortAndAddFileName(int date) {
        if (size == 0) {
            fileNameCache[0] = date;
            return;
        }
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int middle = (low + high) >> 1;
            if (date < fileNameCache[middle]) {
                high = middle - 1;
            } else {
                low = middle + 1;
            }
        }
        //这里需要移动数组，会有一部分性能开销
        for (int i = size - 1; i > high && i <= low; i--) {
            fileNameCache[i + 1] = fileNameCache[i];
        }
        fileNameCache[low] = date;
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > capacity) {
            capacity = capacity + 1000;
            fileNameCache = Arrays.copyOf(fileNameCache, capacity);
        }
    }

    //thread unsafe
    public String getFileNameByIndex(int index) {
        String name = null;
        if (index < size) {
            try {
                Date date = storageFormat.parse("" + fileNameCache[index]);
                name = FILE_PREFIX + displayFormat.format(date) + FILE_POSTFIX;
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return name;
    }

    public String getInputFileNameByIndex(int index) {
        String name = getFileNameByIndex(index);
        if (name != null) {
            name = inputPath + name;
        }
        return name;
    }

    public String getOutputFileNameByIndex(int index) {
        String name = getFileNameByIndex(index);
        if (name != null) {
            name = outputPath + name;
        }
        return name;
    }
}