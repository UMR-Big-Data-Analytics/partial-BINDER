package binder;

import binder.core.BINDER;
import binder.io.DefaultFileInputGenerator;
import binder.io.RelationalFileInput;
import binder.runner.Config;
import binder.utils.CollectionUtils;
import binder.utils.FileUtils;

import java.io.File;
import java.io.IOException;

public class BINDERFile extends BINDER {

    public void setConfig(Config config) {
        this.config = config;
    }

    public void setRelationalInputConfigurationValue2(String identifier, DefaultFileInputGenerator... value) throws IOException {
        if (BINDERFile.Identifier.INPUT_FILES.name().equals(identifier)) {
            this.fileInputGenerator = value;

            this.tableNames = new String[value.length];
            for (int i = 0; i < value.length; i++) {
                RelationalFileInput input = value[i].generateNewCopy();
                this.tableNames[i] = input.relationName();
                input.close();
            }
        } else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(value, ","));
    }

    public void setIntegerConfigurationValue(String identifier, Integer... values) throws IllegalArgumentException {
        if (BINDERFile.Identifier.INPUT_ROW_LIMIT.name().equals(identifier)) {
            if (values.length > 0)
                this.inputRowLimit = values[0];
        } else if (BINDERFile.Identifier.MAX_NARY_LEVEL.name().equals(identifier)) {
            if (values.length > 0)
                this.maxNaryLevel = values[0];
        } else if (BINDERFile.Identifier.NUM_BUCKETS_PER_COLUMN.name().equals(identifier)) {
            if (values[0] <= 0)
                throw new IllegalArgumentException(BINDERFile.Identifier.NUM_BUCKETS_PER_COLUMN.name() + " must be greater than 0!");
            this.numBucketsPerColumn = values[0];
        } else if (BINDERFile.Identifier.MEMORY_CHECK_FREQUENCY.name().equals(identifier)) {
            if (values[0] <= 0)
                throw new IllegalArgumentException(BINDERFile.Identifier.MEMORY_CHECK_FREQUENCY.name() + " must be greater than 0!");
            this.memoryCheckFrequency = values[0];
        } else if (BINDERFile.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name().equals(identifier)) {
            if (values[0] <= 0)
                throw new IllegalArgumentException(BINDERFile.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name() + " must be greater than 0!");
            this.maxMemoryUsagePercentage = values[0];
        } else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
    }

    public void setStringConfigurationValue(String identifier, String... values) throws IllegalArgumentException {
        if (BINDERFile.Identifier.TEMP_FOLDER_PATH.name().equals(identifier)) {
            if ("".equals(values[0]) || " ".equals(values[0]) || "/".equals(values[0]) || "\\".equals(values[0]) || File.separator.equals(values[0]) || FileUtils.isRoot(new File(values[0])))
                throw new IllegalArgumentException(BINDERFile.Identifier.TEMP_FOLDER_PATH + " must not be \"" + values[0] + "\"");
            this.tempFolderPath = values[0];
        } else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
    }

    public void setBooleanConfigurationValue(String identifier, Boolean... values) {
        if (BINDERFile.Identifier.CLEAN_TEMP.name().equals(identifier))
            this.cleanTemp = values[0];
        else if (BINDERFile.Identifier.DETECT_NARY.name().equals(identifier))
            this.detectNary = values[0];
        else if (BINDERFile.Identifier.FILTER_KEY_FOREIGN_KEYS.name().equals(identifier))
            this.nullIsSubset = values[0];
        else
            this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
    }

    protected void handleUnknownConfiguration(String identifier, String value) throws IllegalArgumentException {
        throw new IllegalArgumentException("Unknown configuration: " + identifier + " -> " + value);
    }

    public enum Identifier {
        INPUT_FILES, INPUT_ROW_LIMIT, TEMP_FOLDER_PATH, CLEAN_TEMP, DETECT_NARY, MAX_NARY_LEVEL, FILTER_KEY_FOREIGN_KEYS, NUM_BUCKETS_PER_COLUMN, MEMORY_CHECK_FREQUENCY, MAX_MEMORY_USAGE_PERCENTAGE
    }

}
