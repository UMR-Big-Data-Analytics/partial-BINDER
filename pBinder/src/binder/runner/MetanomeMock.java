package binder.runner;

import binder.BINDERFile;
import binder.core.BINDER;
import binder.io.DefaultFileInputGenerator;
import binder.utils.FileUtils;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;

import java.io.File;
import java.io.IOException;

public class MetanomeMock {

    public static void executeBINDER(Config conf) {
        try {
            BINDER binder;
            DefaultFileInputGenerator[] fileInputGenerators = new DefaultFileInputGenerator[conf.tableNames.length];
            for (int i = 0; i < conf.tableNames.length; i++)
                fileInputGenerators[i] = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(conf.inputFolderPath + conf.databaseName + File.separator + conf.tableNames[i] + conf.inputFileEnding, true, conf.inputFileSeparator, conf.inputFileQuoteChar, conf.inputFileEscape, conf.inputFileStrictQuotes, conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader, conf.inputFileSkipDifferingLines, conf.inputFileNullString));

            BINDERFile binderFile = new BINDERFile();
            binderFile.setRelationalInputConfigurationValue2(BINDERFile.Identifier.INPUT_FILES.name(), fileInputGenerators);
            binderFile.setIntegerConfigurationValue(BINDERFile.Identifier.INPUT_ROW_LIMIT.name(), conf.inputRowLimit);
            binderFile.setStringConfigurationValue(BINDERFile.Identifier.TEMP_FOLDER_PATH.name(), conf.tempFolderPath);
            binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.CLEAN_TEMP.name(), conf.cleanTemp);
            binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.DETECT_NARY.name(), conf.detectNary);
            binderFile.setConfig(conf);
            binder = binderFile;

            long time = System.currentTimeMillis();
            binder.execute();
            time = System.currentTimeMillis() - time;

            if (conf.writeResults) {
                FileUtils.writeToFile(binder + "\r\n\r\n" + "Runtime: " + time + "\r\n\r\n" + conf, conf.measurementsFolderPath + File.separator + conf.statisticsFileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
