package binder.mocks;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.configuration.DbSystem;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import binder.BINDERFile;
import binder.core.BINDER;
import binder.io.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import binder.config.Config;
import binder.utils.FileUtils;

public class MetanomeMock {

	protected static DbSystem mapDatabaseToDbSystem(Config.Database database) {
		switch (database) {
			case DB2: return DbSystem.DB2;
			case MYSQL: return DbSystem.MySQL;
			case POSTGRESQL: return DbSystem.PostgreSQL;
			default: return DbSystem.MySQL;
		}
	}

	public static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator[] relationalInputGenerators) throws InputGenerationException, AlgorithmConfigurationException {
		List<ColumnIdentifier> acceptedColumns = new ArrayList<>();
		for (RelationalInputGenerator relationalInputGenerator: relationalInputGenerators) {
			RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
			String tableName = relationalInput.relationName();
			for (String columnName : relationalInput.columnNames())
				acceptedColumns.add(new ColumnIdentifier(tableName, columnName));
		}
		return acceptedColumns;
    }
	
	public static void executeBINDER(Config conf) {
		try {
			BINDER binder;
			
			DatabaseConnectionGenerator databaseConnectionGenerator = null;
			FileInputGenerator[] fileInputGenerators = new FileInputGenerator[conf.tableNames.length];
			ResultCache resultReceiver = new ResultCache("MetanomeMock", null);

				for (int i = 0; i < conf.tableNames.length; i++)
					fileInputGenerators[i] = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
							conf.inputFolderPath + conf.databaseName + File.separator + conf.tableNames[i] + conf.inputFileEnding, true,
							conf.inputFileSeparator, conf.inputFileQuotechar, conf.inputFileEscape, conf.inputFileStrictQuotes, 
							conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader, conf.inputFileSkipDifferingLines, conf.inputFileNullString));
				
				BINDERFile binderFile = new BINDERFile();
				binderFile.setRelationalInputConfigurationValue(BINDERFile.Identifier.INPUT_FILES.name(), fileInputGenerators);
				binderFile.setIntegerConfigurationValue(BINDERFile.Identifier.INPUT_ROW_LIMIT.name(), Integer.valueOf(conf.inputRowLimit));
				binderFile.setStringConfigurationValue(BINDERFile.Identifier.TEMP_FOLDER_PATH.name(), conf.tempFolderPath);
				binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.CLEAN_TEMP.name(), Boolean.valueOf(conf.cleanTemp));
				binderFile.setBooleanConfigurationValue(BINDERFile.Identifier.DETECT_NARY.name(), Boolean.valueOf(conf.detectNary));
				binderFile.setResultReceiver(resultReceiver);
				binder = binderFile;
			
			long time = System.currentTimeMillis();
			binder.execute();
			time = System.currentTimeMillis() - time;
			
			if (conf.writeResults) {
				FileUtils.writeToFile(binder.toString() + "\r\n\r\n" + "Runtime: " + time + "\r\n\r\n" + conf.toString(), conf.measurementsFolderPath + File.separator + conf.statisticsFileName);
				FileUtils.writeToFile(format(resultReceiver.fetchNewResults()), conf.measurementsFolderPath + File.separator + conf.resultFileName);
			}
		}
		catch (AlgorithmExecutionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static String format(List<Result> results) {
		HashMap<String, List<String>> ref2Deps = new HashMap<String, List<String>>();

		for (Result result : results) {
			InclusionDependency ind = (InclusionDependency) result;
			
			StringBuilder refBuilder = new StringBuilder("(");
			Iterator<ColumnIdentifier> refIterator = ind.getReferenced().getColumnIdentifiers().iterator();
			while (refIterator.hasNext()) {
				refBuilder.append(refIterator.next().toString());
				if (refIterator.hasNext())
					refBuilder.append(",");
				else
					refBuilder.append(")");
			}
			String ref = refBuilder.toString();
			
			StringBuilder depBuilder = new StringBuilder("(");
			Iterator<ColumnIdentifier> depIterator = ind.getDependant().getColumnIdentifiers().iterator();
			while (depIterator.hasNext()) {
				depBuilder.append(depIterator.next().toString());
				if (depIterator.hasNext())
					depBuilder.append(",");
				else
					depBuilder.append(")");
			}
			String dep = depBuilder.toString();
			
			if (!ref2Deps.containsKey(ref))
				ref2Deps.put(ref, new ArrayList<String>());
			ref2Deps.get(ref).add(dep);
		}
		
		StringBuilder builder = new StringBuilder();
		ArrayList<String> referenced = new ArrayList<String>(ref2Deps.keySet());
		Collections.sort(referenced);
		for (String ref : referenced) {
			List<String> dependants = ref2Deps.get(ref);
			Collections.sort(dependants);
			
			if (!dependants.isEmpty())
				builder.append(ref + " > ");
			for (String dependant : dependants)
				builder.append(dependant + "  ");
			if (!dependants.isEmpty())
				builder.append("\r\n");
		}
		return builder.toString();
	}
}
