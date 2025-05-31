package binder.core;

import binder.utils.CollectionUtils;
import binder.utils.DuplicateHandling;
import binder.utils.FileUtils;
import binder.utils.NullHandling;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.*;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.RelaxedInclusionDependencyResultReceiver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class PartialBinder extends PartialBinderAlgorithm implements RelaxedInclusionDependencyAlgorithm, RelationalInputParameterAlgorithm, IntegerParameterAlgorithm, StringParameterAlgorithm, BooleanParameterAlgorithm {

    @Override
    public void execute() throws AlgorithmExecutionException {
        try {
            super.execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<ConfigurationRequirement<?>>(5);
        configs.add(new ConfigurationRequirementRelationalInput(PartialBinder.Identifier.INPUT_FILES.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));

        ConfigurationRequirementString tempFolder = new ConfigurationRequirementString(PartialBinder.Identifier.TEMP_FOLDER_PATH.name());
        String[] defaultTempFolder = new String[1];
        defaultTempFolder[0] = this.tempFolderPath;
        tempFolder.setDefaultValues(defaultTempFolder);
        tempFolder.setRequired(true);
        configs.add(tempFolder);

        ConfigurationRequirementString threshold = new ConfigurationRequirementString(Identifier.THRESHOLD.name());
        String[] thresholdDefault = new String[1];
        thresholdDefault[0] = String.valueOf(this.threshold);
        threshold.setDefaultValues(thresholdDefault);
        threshold.setRequired(true);
        configs.add(threshold);

        ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(PartialBinder.Identifier.INPUT_ROW_LIMIT.name());
        Integer[] defaultInputRowLimit = { Integer.valueOf(this.inputRowLimit) };
        inputRowLimit.setDefaultValues(defaultInputRowLimit);
        inputRowLimit.setRequired(false);
        configs.add(inputRowLimit);

        ConfigurationRequirementInteger maxNaryLevel = new ConfigurationRequirementInteger(PartialBinder.Identifier.MAX_NARY_LEVEL.name());
        Integer[] defaultMaxNaryLevel = { Integer.valueOf(this.maxNaryLevel) };
        maxNaryLevel.setDefaultValues(defaultMaxNaryLevel);
        maxNaryLevel.setRequired(false);
        configs.add(maxNaryLevel);

        ConfigurationRequirementInteger numBucketsPerColumn = new ConfigurationRequirementInteger(PartialBinder.Identifier.NUM_BUCKETS_PER_COLUMN.name());
        Integer[] defaultNumBucketsPerColumn = { Integer.valueOf(this.numBucketsPerColumn) };
        numBucketsPerColumn.setDefaultValues(defaultNumBucketsPerColumn);
        numBucketsPerColumn.setRequired(true);
        configs.add(numBucketsPerColumn);

        ConfigurationRequirementInteger memoryCheckFrequency = new ConfigurationRequirementInteger(PartialBinder.Identifier.MEMORY_CHECK_FREQUENCY.name());
        Integer[] defaultMemoryCheckFrequency = { Integer.valueOf(this.memoryCheckFrequency) };
        memoryCheckFrequency.setDefaultValues(defaultMemoryCheckFrequency);
        memoryCheckFrequency.setRequired(true);
        configs.add(memoryCheckFrequency);

        ConfigurationRequirementInteger maxMemoryUsagePercentage = new ConfigurationRequirementInteger(PartialBinder.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name());
        Integer[] defaultMaxMemoryUsagePercentage = { Integer.valueOf(this.maxMemoryUsagePercentage) };
        maxMemoryUsagePercentage.setDefaultValues(defaultMaxMemoryUsagePercentage);
        maxMemoryUsagePercentage.setRequired(true);
        configs.add(maxMemoryUsagePercentage);

        ConfigurationRequirementBoolean cleanTemp = new ConfigurationRequirementBoolean(PartialBinder.Identifier.CLEAN_TEMP.name());
        Boolean[] defaultCleanTemp = new Boolean[1];
        defaultCleanTemp[0] = Boolean.valueOf(this.cleanTemp);
        cleanTemp.setDefaultValues(defaultCleanTemp);
        cleanTemp.setRequired(true);
        configs.add(cleanTemp);

        ConfigurationRequirementBoolean detectNary = new ConfigurationRequirementBoolean(PartialBinder.Identifier.DETECT_NARY.name());
        Boolean[] defaultDetectNary = new Boolean[1];
        defaultDetectNary[0] = Boolean.valueOf(this.detectNary);
        detectNary.setDefaultValues(defaultDetectNary);
        detectNary.setRequired(true);
        configs.add(detectNary);

        ConfigurationRequirementString nullH = new ConfigurationRequirementString(
                Identifier.NULL_HANDLING.name());
        nullH.setDefaultValues(new String[]{"SUBSET"});
        nullH.setRequired(true);
        configs.add(nullH);

        ConfigurationRequirementString dupH = new ConfigurationRequirementString(
                Identifier.DUPLICATE_HANDLING.name());
        dupH.setDefaultValues(new String[]{"AWARE"});
        dupH.setRequired(true);
        configs.add(dupH);

        return configs;
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
        if (PartialBinder.Identifier.INPUT_FILES.name().equals(identifier)) {
            this.fileInputGenerator = values;

            this.tableNames = new String[fileInputGenerator.length];
            for (int i = 0; i < fileInputGenerator.length; i++) {
                try (RelationalInput input = fileInputGenerator[i].generateNewCopy()){
                    this.tableNames[i] = input.relationName();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(fileInputGenerator, ","));
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) throws IllegalArgumentException {
        if (PartialBinder.Identifier.TEMP_FOLDER_PATH.name().equals(identifier)) {
            isIllegalArgument(values);
            this.tempFolderPath = values[0];
        } else if (Identifier.THRESHOLD.name().equals(identifier)) {
            isIllegalArgument(values);
            this.threshold = Double.parseDouble(values[0]);
        } else if (Identifier.NULL_HANDLING.name().equals(identifier)) {
            this.nullHandling = NullHandling.valueOf(values[0]);
        } else if (Identifier.DUPLICATE_HANDLING.name().equals(identifier)) {
            this.duplicateHandling = DuplicateHandling.valueOf(values[0]);

        } else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
    }

    private void isIllegalArgument(String[] values) {
        if ("".equals(values[0]) || " ".equals(values[0]) || "/".equals(values[0]) || "\\".equals(values[0]) || File.separator.equals(values[0]) || FileUtils.isRoot(new File(values[0])))
            throw new IllegalArgumentException(Identifier.TEMP_FOLDER_PATH + " must not be \"" + values[0] + "\"");
    }

    @Override
    public void setIntegerConfigurationValue(String identifier, Integer... values) {
        if (PartialBinder.Identifier.INPUT_ROW_LIMIT.name().equals(identifier)) {
            if (values.length > 0)
                this.inputRowLimit = values[0].intValue();
        }
        else if (PartialBinder.Identifier.MAX_NARY_LEVEL.name().equals(identifier)) {
            if (values.length > 0)
                this.maxNaryLevel = values[0].intValue();
        }
        else if (PartialBinder.Identifier.NUM_BUCKETS_PER_COLUMN.name().equals(identifier)) {
            this.numBucketsPerColumn = values[0].intValue();
        }
        else if (PartialBinder.Identifier.MEMORY_CHECK_FREQUENCY.name().equals(identifier)) {
            this.memoryCheckFrequency = values[0].intValue();
        }
        else if (PartialBinder.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name().equals(identifier)) {
            this.maxMemoryUsagePercentage = values[0].intValue();
        }
        else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
    }

    @Override
    public void setBooleanConfigurationValue(String identifier, Boolean... values) {
        if (PartialBinder.Identifier.CLEAN_TEMP.name().equals(identifier))
            this.cleanTemp = values[0];
        else if (PartialBinder.Identifier.DETECT_NARY.name().equals(identifier))
            this.detectNary = values[0];
        else if (PartialBinder.Identifier.FILTER_KEY_FOREIGN_KEYS.name().equals(identifier))
            this.nullIsSubset = values[0];
        else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
    }

    protected void handleUnknownConfiguration(String identifier, String value) throws IllegalArgumentException {
        throw new IllegalArgumentException("Unknown configuration: " + identifier + " -> " + value);
    }

    @Override
    public void setResultReceiver(RelaxedInclusionDependencyResultReceiver resultReceiver) {
        this.resultReceiver = resultReceiver;
    }


    @Override
    public String getAuthors() {
        return "Jakob L. Müller, Marcian Seeger and Thorsten Papenbrock";
    }

    @Override
    public String getDescription() {
        return "Partial Divide and Conquer-based IND discovery";
    }

    public enum Identifier {
        INPUT_FILES, INPUT_ROW_LIMIT, TEMP_FOLDER_PATH, CLEAN_TEMP, DETECT_NARY, MAX_NARY_LEVEL, FILTER_KEY_FOREIGN_KEYS, NUM_BUCKETS_PER_COLUMN, MEMORY_CHECK_FREQUENCY, MAX_MEMORY_USAGE_PERCENTAGE, THRESHOLD, NULL_HANDLING, DUPLICATE_HANDLING
    }

}
