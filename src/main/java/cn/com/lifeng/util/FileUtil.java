package cn.com.lifeng.util;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Created by lifeng on 16/3/27.
 */
public class FileUtil {

    public static boolean checkFileExist(String path) {
        File file = new File(path);
        boolean success = true;
        if (!file.exists()) success = false;
        return success;
    }

    public static boolean mkDir(String path) {
        int retryTimes = 3;
        boolean createSuccess = false;
        while (retryTimes > 0) {
            File file = new File(path);
            if (file.exists() || file.mkdirs()) {
                createSuccess = true;
                break;
            }
            retryTimes -= 1;
        }
        return createSuccess;
    }

    public static boolean isAbsolute(String path) {
        File file = new File(path);
        boolean success = true;
        if (!file.isAbsolute()) {
            success = false;
        }
        return success;
    }
}
