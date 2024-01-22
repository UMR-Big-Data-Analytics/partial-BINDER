package binder.runner;
import java.io.File;

public class MetanomeTestRunner {

    public static void run(Config conf, String runLabel) {
        long time = System.currentTimeMillis();
        String algorithmName = conf.algorithm.name();
        String defaultTempFolderPath = conf.tempFolderPath;
        String defaultMeasurementsFolderPath = conf.measurementsFolderPath;

        conf.tempFolderPath = defaultTempFolderPath + File.separator + algorithmName + "_" + runLabel;
        conf.measurementsFolderPath = defaultMeasurementsFolderPath + File.separator + algorithmName + "_" + runLabel;

        MetanomeMock.executeBINDER(conf);

        conf.tempFolderPath = defaultTempFolderPath;
        conf.measurementsFolderPath = defaultMeasurementsFolderPath;

        System.out.println("(" + runLabel + ") Runtime " + algorithmName + ": " + (System.currentTimeMillis() - time) + " ms");
    }

    public static void main(String[] args) {
        MetanomeTestRunner.run(new Config(Config.Algorithm.BINDER, Config.Dataset.KAGGLE), "test");
    }


}
