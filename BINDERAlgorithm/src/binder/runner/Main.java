package binder.runner;

public class Main {

	public static void main(String[] args) {
		MetanomeTestRunner.run(new Config(Config.Algorithm.BINDER, Config.Database.FILE, Config.Dataset.NCVOTER_STATEWIDE_SMALL, -1, -1), "test");
		//MetanomeTestRunner.run(args);
		//MetanomeTestRunner.runRowScalability();
		//MetanomeTestRunner.runOnAllDB2Datasets();
		//MetanomeTestRunner.runOnAllPostgreSQLDatasets();
	}

}
