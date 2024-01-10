package binder.runner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import de.uni_potsdam.hpi.metanome_test_runner.config.Config;
import de.uni_potsdam.hpi.metanome_test_runner.mocks.MetanomeMock;
import de.uni_potsdam.hpi.utils.CollectionUtils;

public class MetanomeTestRunner {

	public static void run() {
		Config conf = new Config(Config.Algorithm.BINDER, Config.Database.FILE, Config.Dataset.NCVOTER_STATEWIDE_SMALL);
		//Config conf = new Config(Config.Algorithm.SPIDER, Config.Database.FILE, Config.Dataset.TEST);
		
		run(conf, "run");
		//runSPIDERandBINDER(conf, "");
	}

	public static void run(Config conf, String runLabel) {
		long time = System.currentTimeMillis();
		String algorithmName = conf.algorithm.name();
		String defaultTempFolderPath = conf.tempFolderPath;
		String defaultMeasurementsFolderPath = conf.measurementsFolderPath;
		
		conf.tempFolderPath = defaultTempFolderPath + File.separator + algorithmName + "_" + runLabel;
		conf.measurementsFolderPath = defaultMeasurementsFolderPath + File.separator + algorithmName + "_" + runLabel;
		
		switch(conf.algorithm) {
			case SPIDER:
				MetanomeMock.executeSPIDER(conf);
				break;
			case BINDER:
				MetanomeMock.executeBINDER(conf);
				break;
			case BINDERpp:
				MetanomeMock.executeBINDERpp(conf);
				break;
			default:
				System.out.println("Algorithm specification unknown: " + conf.algorithm.name());
		}
		
		conf.tempFolderPath = defaultTempFolderPath;
		conf.measurementsFolderPath = defaultMeasurementsFolderPath;
		
		System.out.println("(" + runLabel + ") Runtime " + algorithmName + ": " + (System.currentTimeMillis() - time) + " ms");
	}

	public static void run(String[] args) {
		if (args.length != 4)
			wrongArguments(args);
		
		Config.Algorithm algorithm = null;
		String algorithmArg = args[0].toLowerCase();
		for (Config.Algorithm possibleAlgorithm : Config.Algorithm.values())
			if (possibleAlgorithm.name().toLowerCase().equals(algorithmArg))
				algorithm = possibleAlgorithm;
		
		Config.Database database = null;
		String databaseArg = args[1].toLowerCase();
		for (Config.Database possibleDatabase : Config.Database.values())
			if (possibleDatabase.name().toLowerCase().equals(databaseArg))
				database = possibleDatabase;
		
		Config.Dataset dataset = null;
		String datasetArg = args[2].toLowerCase();
		for (Config.Dataset possibleDataset : Config.Dataset.values())
			if (possibleDataset.name().toLowerCase().equals(datasetArg))
				dataset = possibleDataset;

		int inputTableLimit = Integer.valueOf(args[3]).intValue();
		
		if ((algorithm == null) || (database == null) || (dataset == null))
			wrongArguments(args);
		
		Config conf = new Config(algorithm, database, dataset, inputTableLimit, -1);
		
		run(conf, CollectionUtils.concat(args, "_"));
	}
	
	private static void wrongArguments(String[] args) {
		StringBuilder message = new StringBuilder();
		message.append("\r\nArguments not supported: " + CollectionUtils.concat(args, " "));
		message.append("\r\nProvide correct values: <algorithm> <database> <dataset> <inputTableLimit>");
		throw new RuntimeException(message.toString());
	}
	
	public static void runSPIDERandBINDER(Config conf, String runLabel) {
		System.gc();
		{
			conf.algorithm = Config.Algorithm.SPIDER;
			run(conf, runLabel);
		}
		System.gc();
		{
			conf.algorithm = Config.Algorithm.BINDER;
			run(conf, runLabel);
		}
	}
	
	public static void runRowScalability() {
		int[] inputSizes = {1000000, 2000000, 3000000, 4000000, 5000000, 6000000};
		
		for (int inputSize : inputSizes) {
			Config conf = new Config(Config.Algorithm.BINDER, Config.Database.POSTGRESQL, Config.Dataset.TPC_H, -1, inputSize);
			conf.tempFolderPath = conf.tempFolderPath + File.separator + inputSize;
			conf.measurementsFolderPath = conf.measurementsFolderPath + File.separator + inputSize;

			runSPIDERandBINDER(conf, String.valueOf(inputSize));
		}
	}

	public static void runOnAllDB2Datasets() {
		Config conf = new Config();
		String defaultTempFolderPath = conf.tempFolderPath;
		String defaultMeasurementsFolderPath = conf.measurementsFolderPath;
		
		List<Config.Dataset> datasets = new ArrayList<Config.Dataset>();
		datasets.add(Config.Dataset.BIOSQLSP);
		datasets.add(Config.Dataset.CATH);
		datasets.add(Config.Dataset.CENSUS);
		datasets.add(Config.Dataset.COMA);
		datasets.add(Config.Dataset.EMDE);
		datasets.add(Config.Dataset.ENSEMBL);
		datasets.add(Config.Dataset.LOD);
		datasets.add(Config.Dataset.PDB);
		datasets.add(Config.Dataset.SCOP);
		datasets.add(Config.Dataset.TESMA);
		datasets.add(Config.Dataset.WIKIPEDIA);
		datasets.add(Config.Dataset.WIKIRANK);
		
		for (Config.Dataset dataset : datasets) {
			conf.setSource(Config.Database.DB2, dataset);
			conf.tempFolderPath = defaultTempFolderPath + File.separator + conf.databaseName;
			conf.measurementsFolderPath = defaultMeasurementsFolderPath + File.separator + conf.databaseName;
			runSPIDERandBINDER(conf, conf.databaseName);
		}
	}

	public static void runOnAllPostgreSQLDatasets() {
		Config conf = new Config();
		String defaultTempFolderPath = conf.tempFolderPath;
		String defaultMeasurementsFolderPath = conf.measurementsFolderPath;
		
		List<Config.Dataset> datasets = new ArrayList<Config.Dataset>();
		datasets.add(Config.Dataset.PLISTA_LARGE);
		datasets.add(Config.Dataset.PLISTA_SMALL);
		datasets.add(Config.Dataset.TPC_H);
		
		for (Config.Dataset dataset : datasets) {
			conf.setSource(Config.Database.POSTGRESQL, dataset);
			conf.tempFolderPath = defaultTempFolderPath + File.separator + conf.databaseName;
			conf.measurementsFolderPath = defaultMeasurementsFolderPath + File.separator + conf.databaseName;
			runSPIDERandBINDER(conf, conf.databaseName);
		}
	}

	public static void runNewApproach() {
		Config conf = new Config(Config.Algorithm.BINDER, Config.Database.FILE, Config.Dataset.PLISTA);
		
		runSPIDERandBINDER(conf, "");
	}

}
