package binder.utils;

import java.io.*;
import java.nio.charset.Charset;

public class FileUtils {

    public static String CHARSET_NAME = "UTF-8";

    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean isRoot(File directory) {
        File rootSlash = new File("/");
        File rootBackslash = new File("\\");
        return directory.getAbsolutePath().equals(rootSlash.getAbsolutePath()) || directory.getAbsolutePath().equals(rootBackslash.getAbsolutePath());
    }

    public static void deleteDirectory(File directory) {
        if (isRoot(directory)) return;

        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (File file : files) {
                    if (file.isDirectory()) deleteDirectory(file);
                    else file.delete();
                }
            }
        }
        directory.delete();
    }

    public static void cleanDirectory(File directory) {
        if (isRoot(directory)) return;

        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (File file : files) {
                    if (file.isDirectory()) deleteDirectory(file);
                    else file.delete();
                }
            }
        }
    }

    public static void createFile(String filePath, boolean recreateIfExists) throws IOException {
        File file = new File(filePath);
        File folder = file.getParentFile();

        if ((folder != null) && !folder.exists()) {
            folder.mkdirs();
            while (!folder.exists()) {
            }
        }

        if (recreateIfExists && file.exists()) file.delete();

        if (!file.exists()) {
            file.createNewFile();
            while (!file.exists()) {
            }
        }
    }

    public static BufferedReader buildFileReader(String filePath) throws FileNotFoundException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(filePath), Charset.forName(FileUtils.CHARSET_NAME)));
    }

    public static BufferedWriter buildFileWriter(String filePath, boolean append) throws IOException {
        FileUtils.createFile(filePath, !append);
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, append), Charset.forName(FileUtils.CHARSET_NAME)));
    }

    public static void writeToFile(String content, String filePath) throws IOException {
        try (Writer writer = FileUtils.buildFileWriter(filePath, false)) {
            writer.write(content);
        }
    }

}
