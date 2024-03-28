package binder.runner;

import binder.core.BINDERFile;
import binder.core.BINDER;
import binder.io.DefaultFileInputGenerator;
import binder.utils.FileUtils;

import java.io.File;
import java.io.IOException;

public class Runner {

    public static void main(String[] args) {
        Runner.run(new Config(Config.Dataset.TPCH_1, 1.0, Config.NullHandling.SUBSET, Config.DuplicateHandling.AWARE), "test");
    }

    public static void run(Config conf, String runLabel) {
        long time = System.currentTimeMillis();
        String defaultTempFolderPath = conf.tempFolder;
        String defaultMeasurementsFolderPath = conf.resultFolder;

        executeBINDER(conf);

        conf.tempFolder = defaultTempFolderPath;
        conf.resultFolder = defaultMeasurementsFolderPath;

        System.out.println("(" + runLabel + ") Runtime: " + (System.currentTimeMillis() - time) + " ms");
    }


    public static void executeBINDER(Config conf) {
        try {
            BINDER binder;
            DefaultFileInputGenerator[] fileInputGenerators = new DefaultFileInputGenerator[conf.relationNames.length];
            for (int i = 0; i < conf.relationNames.length; i++)
                fileInputGenerators[i] = new DefaultFileInputGenerator(conf, i);

            BINDERFile binderFile = new BINDERFile();
            binderFile.setRelationalInputConfigurationValue2(BINDERFile.Identifier.INPUT_FILES.name(), fileInputGenerators);
            binderFile.setStringConfigurationValue(BINDERFile.Identifier.TEMP_FOLDER_PATH.name(), conf.tempFolder);
            binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.CLEAN_TEMP.name(), conf.cleanTemp);
            binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.DETECT_NARY.name(), conf.detectNary);
            binderFile.setConfig(conf);
            binder = binderFile;

            long time = System.currentTimeMillis();
            binder.execute();
            time = System.currentTimeMillis() - time;

            if (conf.writeResults) {
                FileUtils.writeToFile(binder + "\r\n\r\n" + "Runtime: " + time + "\r\n\r\n" + conf, conf.resultFolder + File.separator + conf.statisticsFileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
