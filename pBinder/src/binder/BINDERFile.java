package binder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import binder.io.DefaultFileInputGenerator;
import binder.io.RelationalFileInput;
import binder.runner.Config;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementString;
import de.metanome.algorithm_integration.input.*;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import binder.core.BINDER;
import binder.utils.CollectionUtils;
import binder.utils.FileUtils;

public class BINDERFile extends BINDER implements InclusionDependencyAlgorithm, RelationalInputParameterAlgorithm, IntegerParameterAlgorithm, StringParameterAlgorithm, BooleanParameterAlgorithm {

	public enum Identifier {
		INPUT_FILES, INPUT_ROW_LIMIT, TEMP_FOLDER_PATH, CLEAN_TEMP, DETECT_NARY, MAX_NARY_LEVEL, FILTER_KEY_FOREIGN_KEYS, NUM_BUCKETS_PER_COLUMN, MEMORY_CHECK_FREQUENCY, MAX_MEMORY_USAGE_PERCENTAGE
	}

	public void setConfig(Config config) {
		this.config = config;
	}
	
	@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
		ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<>(5);
		configs.add(new ConfigurationRequirementRelationalInput(BINDERFile.Identifier.INPUT_FILES.name(), ConfigurationRequirement.ARBITRARY_NUMBER_OF_VALUES));
		
		ConfigurationRequirementString tempFolder = new ConfigurationRequirementString(BINDERFile.Identifier.TEMP_FOLDER_PATH.name());
		String[] defaultTempFolder = new String[1];
		defaultTempFolder[0] = this.tempFolderPath;
		tempFolder.setDefaultValues(defaultTempFolder);
		tempFolder.setRequired(true);
		configs.add(tempFolder);
		
		ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(BINDERFile.Identifier.INPUT_ROW_LIMIT.name());
		Integer[] defaultInputRowLimit = {this.inputRowLimit};
		inputRowLimit.setDefaultValues(defaultInputRowLimit);
		inputRowLimit.setRequired(false);
		configs.add(inputRowLimit);
		
		ConfigurationRequirementInteger maxNaryLevel = new ConfigurationRequirementInteger(BINDERFile.Identifier.MAX_NARY_LEVEL.name());
		Integer[] defaultMaxNaryLevel = {this.maxNaryLevel};
		maxNaryLevel.setDefaultValues(defaultMaxNaryLevel);
		maxNaryLevel.setRequired(false);
		configs.add(maxNaryLevel);
		
		ConfigurationRequirementInteger numBucketsPerColumn = new ConfigurationRequirementInteger(BINDERFile.Identifier.NUM_BUCKETS_PER_COLUMN.name());
		Integer[] defaultNumBucketsPerColumn = {this.numBucketsPerColumn};
		numBucketsPerColumn.setDefaultValues(defaultNumBucketsPerColumn);
		numBucketsPerColumn.setRequired(true);
		configs.add(numBucketsPerColumn);

		ConfigurationRequirementInteger memoryCheckFrequency = new ConfigurationRequirementInteger(BINDERFile.Identifier.MEMORY_CHECK_FREQUENCY.name());
		Integer[] defaultMemoryCheckFrequency = {this.memoryCheckFrequency};
		memoryCheckFrequency.setDefaultValues(defaultMemoryCheckFrequency);
		memoryCheckFrequency.setRequired(true);
		configs.add(memoryCheckFrequency);

		ConfigurationRequirementInteger maxMemoryUsagePercentage = new ConfigurationRequirementInteger(BINDERFile.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name());
		Integer[] defaultMaxMemoryUsagePercentage = {this.maxMemoryUsagePercentage};
		maxMemoryUsagePercentage.setDefaultValues(defaultMaxMemoryUsagePercentage);
		maxMemoryUsagePercentage.setRequired(true);
		configs.add(maxMemoryUsagePercentage);
		
		ConfigurationRequirementBoolean cleanTemp = new ConfigurationRequirementBoolean(BINDERFile.Identifier.CLEAN_TEMP.name());
		Boolean[] defaultCleanTemp = new Boolean[1];
		defaultCleanTemp[0] = this.cleanTemp;
		cleanTemp.setDefaultValues(defaultCleanTemp);
		cleanTemp.setRequired(true);
		configs.add(cleanTemp);
		
		ConfigurationRequirementBoolean detectNary = new ConfigurationRequirementBoolean(BINDERFile.Identifier.DETECT_NARY.name());
		Boolean[] defaultDetectNary = new Boolean[1];
		defaultDetectNary[0] = this.detectNary;
		detectNary.setDefaultValues(defaultDetectNary);
		detectNary.setRequired(true);
		configs.add(detectNary);

		ConfigurationRequirementBoolean filterKeyForeignKeys = new ConfigurationRequirementBoolean(BINDERFile.Identifier.FILTER_KEY_FOREIGN_KEYS.name());
		Boolean[] defaultFilterKeyForeignKeys = new Boolean[1];
		defaultFilterKeyForeignKeys[0] = this.nullIsSubset;
		filterKeyForeignKeys.setDefaultValues(defaultFilterKeyForeignKeys);
		filterKeyForeignKeys.setRequired(true);
		configs.add(filterKeyForeignKeys);
		
		return configs;
	}

	public void setRelationalInputConfigurationValue2(String identifier, DefaultFileInputGenerator... value) throws AlgorithmConfigurationException, IOException {
		if (BINDERFile.Identifier.INPUT_FILES.name().equals(identifier)) {
			this.fileInputGenerator = value;

			this.tableNames = new String[value.length];
			RelationalFileInput input = null;
			for (int i = 0; i < value.length; i++) {
					input = value[i].generateNewCopy();
					this.tableNames[i] = input.relationName();
					input.close();
			}
		}
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(value, ","));
	}

	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... value) throws AlgorithmConfigurationException {
		return;
	}

	@Override
	public void setResultReceiver(InclusionDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
		if (BINDERFile.Identifier.INPUT_ROW_LIMIT.name().equals(identifier)) {
			if (values.length > 0)
				this.inputRowLimit = values[0];
		}
		else if (BINDERFile.Identifier.MAX_NARY_LEVEL.name().equals(identifier)) {
			if (values.length > 0)
				this.maxNaryLevel = values[0];
		}
		else if (BINDERFile.Identifier.NUM_BUCKETS_PER_COLUMN.name().equals(identifier)) {
			if (values[0] <= 0)
				throw new AlgorithmConfigurationException(BINDERFile.Identifier.NUM_BUCKETS_PER_COLUMN.name() + " must be greater than 0!");
			this.numBucketsPerColumn = values[0];
		}
		else if (BINDERFile.Identifier.MEMORY_CHECK_FREQUENCY.name().equals(identifier)) {
			if (values[0] <= 0)
				throw new AlgorithmConfigurationException(BINDERFile.Identifier.MEMORY_CHECK_FREQUENCY.name() + " must be greater than 0!");
			this.memoryCheckFrequency = values[0];
		}
		else if (BINDERFile.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name().equals(identifier)) {
			if (values[0] <= 0)
				throw new AlgorithmConfigurationException(BINDERFile.Identifier.MAX_MEMORY_USAGE_PERCENTAGE.name() + " must be greater than 0!");
			this.maxMemoryUsagePercentage = values[0];
		}
		else 
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	@Override
	public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
		if (BINDERFile.Identifier.TEMP_FOLDER_PATH.name().equals(identifier)) {
			if ("".equals(values[0]) || " ".equals(values[0]) || "/".equals(values[0]) || "\\".equals(values[0]) || File.separator.equals(values[0]) || FileUtils.isRoot(new File(values[0])))
				throw new AlgorithmConfigurationException(BINDERFile.Identifier.TEMP_FOLDER_PATH + " must not be \"" + values[0] + "\"");
			this.tempFolderPath = values[0];
		}
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
		if (BINDERFile.Identifier.CLEAN_TEMP.name().equals(identifier))
			this.cleanTemp = values[0];
		else if (BINDERFile.Identifier.DETECT_NARY.name().equals(identifier))
			this.detectNary = values[0];
		else if (BINDERFile.Identifier.FILTER_KEY_FOREIGN_KEYS.name().equals(identifier))
			this.nullIsSubset = values[0];
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	protected void handleUnknownConfiguration(String identifier, String value) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> " + value);
	}
	
	@Override
	public String getAuthors() {
		return this.getAuthorName();
	}

	@Override
	public String getDescription() {
		return this.getDescriptionText();
	}

}
