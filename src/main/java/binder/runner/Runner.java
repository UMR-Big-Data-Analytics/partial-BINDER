package binder.runner;

import binder.core.PartialBinder;
import binder.utils.FileUtils;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.backend.input.file.DefaultFileInputGenerator;

import java.io.File;
import java.io.IOException;

public class Runner {

    static String DATA_FOLDER = "F:\\metaserve\\io\\data\\";
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            Config config = new Config(1.0);
            config.hasHeader = true;
            config.fileEnding = ".csv";
            config.separator = ',';
            config.setDataset(DATA_FOLDER + "Cars");
            config.tempFolder = System.getProperty("java.io.tmpdir");
            config.resultFolder = System.getProperty("java.io.tmpdir");
            config.datasetFolder = DATA_FOLDER;

            long time = System.currentTimeMillis();
            executeBINDER(config, 1);
            System.out.println("Runtime: " + (System.currentTimeMillis() - time) + " ms");
        } else {
            // Parse arguments
            String datasetName = args[0];
            char separator = args[1].charAt(0);
            String fileEnding = String.valueOf(args[2]);
            boolean inputFileHasHeader = Boolean.parseBoolean(args[3]);
            double threshold = Double.parseDouble(args[4]);
            int maxNary = Integer.parseInt(args[5]);
            int NUMBER_OF_FILES = Integer.parseInt(args[11]);

            // Create and configure the Config object
            Config config = new Config(threshold);
            config.hasHeader = inputFileHasHeader;
            config.fileEnding = fileEnding;
            config.separator = separator;
            if(datasetName.equals("Musicbrainz")) {
                config.fileEnding = "";
                config.nullString = "\\N";
                config.quoteChar = '\0';
            }
            config.setDataset(DATA_FOLDER + datasetName, NUMBER_OF_FILES);
            config.tempFolder = System.getProperty("java.io.tmpdir");
            config.resultFolder = System.getProperty("java.io.tmpdir");
            config.datasetFolder = DATA_FOLDER;

            long time = System.currentTimeMillis();
            executeBINDER(config, maxNary);
            System.out.println("Runtime: " + (System.currentTimeMillis() - time) + " ms");
        }

    }

    public static void executeBINDER(Config conf, int maxNary) {
        try {

            DefaultFileInputGenerator[] fileInputGenerators = new DefaultFileInputGenerator[conf.relationNames.length];
            for (int i = 0; i < conf.relationNames.length; i++) {
                File file = new File(conf.datasetFolder + conf.datasetName + File.separator + conf.relationNames[i] + conf.fileEnding);
                fileInputGenerators[i] = new DefaultFileInputGenerator(file, conf.toConfigurationSettingFileInput(file.getName()));
            }

            PartialBinder binderFile = new PartialBinder();
            binderFile.setRelationalInputConfigurationValue(PartialBinder.Identifier.INPUT_FILES.name(), fileInputGenerators);
            binderFile.setStringConfigurationValue(PartialBinder.Identifier.TEMP_FOLDER_PATH.name(), conf.tempFolder);
            binderFile.setStringConfigurationValue(PartialBinder.Identifier.THRESHOLD.name(), "1.0");
            binderFile.setBooleanConfigurationValue(PartialBinder.Identifier.CLEAN_TEMP.name(), conf.cleanTemp);
            binderFile.setBooleanConfigurationValue(PartialBinder.Identifier.DETECT_NARY.name(), conf.detectNary);
            binderFile.setIntegerConfigurationValue(PartialBinder.Identifier.MAX_NARY_LEVEL.name(), maxNary);

            long time = System.currentTimeMillis();
            binderFile.execute();
            time = System.currentTimeMillis() - time;

            if (conf.writeResults) {
                FileUtils.writeToFile(binderFile + "\r\n\r\n" + "Runtime: " + time + "\r\n\r\n" + conf, conf.resultFolder + File.separator + conf.getResultName());
            }
        } catch (IOException | AlgorithmExecutionException e) {
            e.printStackTrace();
        }
    }

}
