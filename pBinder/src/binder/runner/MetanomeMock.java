package binder.runner;

import binder.BINDERFile;
import binder.core.BINDER;
import binder.io.DefaultFileInputGenerator;
import binder.utils.FileUtils;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.result_receiver.ResultCache;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class MetanomeMock {

    public static void executeBINDER(Config conf) {
        try {
            BINDER binder;
            DefaultFileInputGenerator[] fileInputGenerators = new DefaultFileInputGenerator[conf.tableNames.length];
            ResultCache resultReceiver = new ResultCache("MetanomeMock", null);
            for (int i = 0; i < conf.tableNames.length; i++)
                fileInputGenerators[i] = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(conf.inputFolderPath + conf.databaseName + File.separator + conf.tableNames[i] + conf.inputFileEnding, true, conf.inputFileSeparator, conf.inputFileQuoteChar, conf.inputFileEscape, conf.inputFileStrictQuotes, conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader, conf.inputFileSkipDifferingLines, conf.inputFileNullString));

            BINDERFile binderFile = new BINDERFile();
            binderFile.setRelationalInputConfigurationValue2(BINDERFile.Identifier.INPUT_FILES.name(), fileInputGenerators);
            binderFile.setIntegerConfigurationValue(BINDERFile.Identifier.INPUT_ROW_LIMIT.name(), conf.inputRowLimit);
            binderFile.setStringConfigurationValue(BINDERFile.Identifier.TEMP_FOLDER_PATH.name(), conf.tempFolderPath);
            binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.CLEAN_TEMP.name(), conf.cleanTemp);
            binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.DETECT_NARY.name(), conf.detectNary);
            binderFile.setResultReceiver(resultReceiver);
            binderFile.setConfig(conf);
            binder = binderFile;

            long time = System.currentTimeMillis();
            binder.execute();
            time = System.currentTimeMillis() - time;

            if (conf.writeResults) {
                FileUtils.writeToFile(binder + "\r\n\r\n" + "Runtime: " + time + "\r\n\r\n" + conf, conf.measurementsFolderPath + File.separator + conf.statisticsFileName);
                FileUtils.writeToFile(format(resultReceiver.fetchNewResults()), conf.measurementsFolderPath + File.separator + conf.resultFileName);
            }
        } catch (AlgorithmExecutionException | IOException e) {
            e.printStackTrace();
        }
    }

    private static String format(List<Result> results) {
        HashMap<String, List<String>> ref2Deps = new HashMap<>();

        for (Result result : results) {
            InclusionDependency ind = (InclusionDependency) result;

            StringBuilder refBuilder = new StringBuilder("(");
            Iterator<ColumnIdentifier> refIterator = ind.getReferenced().getColumnIdentifiers().iterator();
            while (refIterator.hasNext()) {
                refBuilder.append(refIterator.next().toString());
                if (refIterator.hasNext()) refBuilder.append(",");
                else refBuilder.append(")");
            }
            String ref = refBuilder.toString();

            StringBuilder depBuilder = new StringBuilder("(");
            Iterator<ColumnIdentifier> depIterator = ind.getDependant().getColumnIdentifiers().iterator();
            while (depIterator.hasNext()) {
                depBuilder.append(depIterator.next().toString());
                if (depIterator.hasNext()) depBuilder.append(",");
                else depBuilder.append(")");
            }
            String dep = depBuilder.toString();

            if (!ref2Deps.containsKey(ref)) ref2Deps.put(ref, new ArrayList<String>());
            ref2Deps.get(ref).add(dep);
        }

        StringBuilder builder = new StringBuilder();
        ArrayList<String> referenced = new ArrayList<>(ref2Deps.keySet());
        Collections.sort(referenced);
        for (String ref : referenced) {
            List<String> dependants = ref2Deps.get(ref);
            Collections.sort(dependants);

            if (!dependants.isEmpty()) builder.append(ref).append(" > ");
            for (String dependant : dependants)
                builder.append(dependant).append("  ");
            if (!dependants.isEmpty()) builder.append("\r\n");
        }
        return builder.toString();
    }
}
