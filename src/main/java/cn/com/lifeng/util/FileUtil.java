package cn.com.lifeng.util;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Created by lifeng on 16/3/27.
 */
public class FileUtil {

    public static void checkFileExist(String path) throws FileNotFoundException {
        File file = new File(path);
        if (!file.exists()) {
            throw new FileNotFoundException(path);
        }
    }

    public static void mkDir(String path) throws Exception {
        if (path.equals(".")) return;
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
        if (!createSuccess) throw new Exception("Path:" + path + " can not be initial");
    }

    public static void isAbsolute(String path) throws Exception {
        File file = new File(path);
        if (!file.isAbsolute()) {
            throw new Exception("Path:" + path + " is not absolute");
        }
    }
}
