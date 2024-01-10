package binder.runner;

import binder.utils.CollectionUtils;

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

    private static void wrongArguments(String[] args) {
        StringBuilder message = new StringBuilder();
        message.append("\r\nArguments not supported: " + CollectionUtils.concat(args, " "));
        message.append("\r\nProvide correct values: <algorithm> <database> <dataset> <inputTableLimit>");
        throw new RuntimeException(message.toString());
    }

}
