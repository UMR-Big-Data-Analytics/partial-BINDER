package binder.runner;

import java.io.File;

public class Config {

    // input path settings
    public String datasetFolder = "D:\\MA\\data" + File.separator;
    public String datasetName;
    public String[] relationNames;

    // file parsing settings
    public String fileEnding = ".csv";
    public char separator = ',';
    public char quoteChar = '\"';
    public char escapeChar = '\\';
    public boolean strictQuotes = false;
    public boolean ignoreLeadingWhiteSpaces = true;
    public boolean hasHeader = true;
    public boolean skipDifferingLines = true; // Skip lines that differ from the dataset's schema
    public String nullString = "";

    // different handling options
    public double threshold;
    public NullHandling nullHandling;
    public DuplicateHandling duplicateHandling;

    // output related settings
    public String tempFolder = ".\\temp";
    public String resultFolder = ".\\results";
    public String statisticsFileName = "IND_statistics.txt";
    public boolean writeResults = true;
    public boolean cleanTemp = true;
    public boolean detectNary = true;


    public Config(Config.Dataset dataset, double threshold, NullHandling nullHandling, DuplicateHandling duplicateHandling) {
        this.setDataset(dataset);
        this.threshold = threshold;
        this.nullHandling = nullHandling;
        this.duplicateHandling = duplicateHandling;
    }

    private void setDataset(Config.Dataset dataset) {
        switch (dataset) {
            case KAGGLE -> {
                this.datasetName = "Kaggle";
                this.relationNames = new String[]{"enrollement_schoolmanagement_2", "data", "amazon_laptop_prices_v01", "IQ_level", "Employee", "employee_data (1)"};
                this.separator = ',';
                this.hasHeader = true;
            }
            case TPCH_1 -> {
                this.datasetName = "TPCH_1";
                this.relationNames = new String[]{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
                this.separator = '|';
                this.hasHeader = false;
                this.strictQuotes = false;
                this.fileEnding = ".tbl";
            }
            case DATA_GOV -> {
                this.datasetName = "data.gov";
                this.relationNames = new String[]{"Air_Quality", "Air_Traffic_Passenger_Statistics",
                        "Crash_Reporting_-_Drivers_Data", "Crime_Data_from_2020_to_Present", "Demographic_Statistics_By_Zip_Code",
                        "diabetes_all_2016", "Electric_Vehicle_Population_Data", "iou_zipcodes_2020",
                        "Lottery_Mega_Millions_Winning_Numbers__Beginning_2002", "Lottery_Powerball_Winning_Numbers__Beginning_2010",
                        "Motor_Vehicle_Collisions_-_Crashes", "National_Obesity_By_State",
                        "NCHS_-_Death_rates_and_life_expectancy_at_birth", "Popular_Baby_Names", "Real_Estate_Sales_2001-2020_GL",
                        "Traffic_Crashes_-_Crashes", "Warehouse_and_Retail_Sales"
                };
                this.separator = ',';
                this.hasHeader = true;
                this.fileEnding = ".csv";
            }
            case UEFA -> {
                this.datasetName = "uefa";
                this.relationNames = new String[]{"attacking", "attempts", "defending", "disciplinary", "distributon",
                        "goalkeeping", "goals", "key_stats"
                };
                this.separator = ',';
                this.hasHeader = true;
                this.fileEnding = ".csv";
            }
            case TEST -> {
                this.datasetName = "wrong_binder_nary";
                this.relationNames = new String[]{"test"};
                this.fileEnding = ".csv";
                this.hasHeader = true;
            }
            default -> {
            }
        }
    }

    public enum Dataset {
        TPCH_1, KAGGLE, DATA_GOV, UEFA, TEST
    }

    public enum NullHandling {
        SUBSET, FOREIGN, EQUALITY, INEQUALITY
    }

    public enum DuplicateHandling {
        AWARE, UNAWARE
    }
}
