package binder.runner;

public class MetanomeTestRunner {

    public static void run(Config conf, String runLabel) {
        long time = System.currentTimeMillis();
        String defaultTempFolderPath = conf.tempFolder;
        String defaultMeasurementsFolderPath = conf.resultFolder;

        MetanomeMock.executeBINDER(conf);

        conf.tempFolder = defaultTempFolderPath;
        conf.resultFolder = defaultMeasurementsFolderPath;

        System.out.println("(" + runLabel + ") Runtime: " + (System.currentTimeMillis() - time) + " ms");
    }

    public static void main(String[] args) {
        MetanomeTestRunner.run(new Config(Config.Dataset.TPCH_1, 1.0, Config.NullHandling.SUBSET, Config.DuplicateHandling.AWARE), "test");
    }

}
