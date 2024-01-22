package binder.runner;

import binder.utils.CollectionUtils;

import java.io.File;

public class Config {

    public Config.Algorithm algorithm;
    public String databaseName;
    public String[] tableNames;
    public int inputRowLimit; // although specifying the row limit in % is more accurate as it uniformly shrinks a dataset, it is still given in #rows, because % would introduce an unfair overhead to SPIDER (you need to count the row numbers of all tables first to perform % on them)
    public String inputFolderPath = "M:\\MA\\data" + File.separator;
    public String inputFileEnding = ".csv";
    public char inputFileSeparator = ',';
    public char inputFileQuoteChar = '\"';
    public char inputFileEscape = '\\';//'\0';//
    public int inputFileSkipLines = 0;
    public boolean inputFileStrictQuotes = true;
    public boolean inputFileIgnoreLeadingWhiteSpace = true;
    public boolean inputFileHasHeader = true;
    public boolean inputFileSkipDifferingLines = true; // Skip lines that differ from the dataset's schema
    public String inputFileNullString = "";
    public String tempFolderPath = "io" + File.separator + "temp";
    public String measurementsFolderPath = "io" + File.separator + "measurements"; // + "BINDER" + File.separator;
    public String statisticsFileName = "IND_statistics.txt";
    public String resultFileName = "IND_results.txt";
    public boolean writeResults = true;
    public boolean cleanTemp = true;
    public boolean detectNary = false;


    public Config(Config.Algorithm algorithm, Config.Dataset dataset) {
        this.algorithm = algorithm;
        this.setDataset(dataset);
    }

    private void setDataset(Config.Dataset dataset) {
        switch (dataset) {
            case KAGGLE:
                this.databaseName = "Kaggle\\";
                this.tableNames = new String[]{"enrollement_schoolmanagement_2", "data", "amazon_laptop_prices_v01", "IQ_level", "Employee", "employee_data (1)"};
                this.inputFileSeparator = ',';
                this.inputFileHasHeader = true;
            case TPCH:
                this.databaseName = "TPCH\\";
                this.tableNames = new String[]{"tpch_customer", "tpch_lineitem", "tpch_nation", "tpch_orders", "tpch_part", "tpch_region", "tpch_supplier"};
                this.inputFileSeparator = ';';
                this.inputFileHasHeader = true;
            default:
                break;
        }
    }

    @Override
    public String toString() {
        return "Config:\r\n\t" + "databaseName: " + this.databaseName + "\r\n\t" + "tableNames: " + CollectionUtils.concat(this.tableNames, ",") + "\r\n\t" + "tempFolderPath" + this.tempFolderPath + "\r\n\t" + "cleanTemp" + this.cleanTemp + "\r\n\t" + "detectNary" + this.detectNary;
    }

    public enum Algorithm {
        BINDER
    }

    public enum Dataset {
        TPCH, KAGGLE
    }
}
