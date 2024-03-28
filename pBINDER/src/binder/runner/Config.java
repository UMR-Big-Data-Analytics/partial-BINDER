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

    private void setDataset(Dataset dataset) {
        switch (dataset) {
            case ANIMAL_CROSSING -> {
                this.datasetName = "Kaggle\\animal-crossing-new-horizons-nookplaza-dataset";
                this.relationNames = new String[]{"accessories", "achievements", "art", "bags", "bottoms", "construction", "dress-up", "fencing", "fish", "floors", "fossils",
                        "headwear", "housewares", "insects", "miscellaneous", "music", "other", "photos", "posters", "reactions", "recipes", "rugs", "shoes", "socks", "tools",
                        "tops", "umbrellas", "villagers", "wall-mounted", "wallpaper"};
            }
            case TPCH_1 -> {
                this.datasetName = "TPCH_1";
                this.relationNames = new String[]{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
                this.separator = '|';
                this.hasHeader = false;
                this.fileEnding = ".tbl";
            }
            case DATA_GOV -> {
                this.datasetName = "data.gov";
                this.relationNames = new String[]{"Air_Quality", "Air_Traffic_Passenger_Statistics", "Crash_Reporting_" + "-_Drivers_Data", "Crime_Data_from_2020_to_Present",
                        "Demographic_Statistics_By_Zip_Code", "diabetes_all_2016", "Electric_Vehicle_Population_Data", "iou_zipcodes_2020",
                        "Lottery_Mega_Millions_Winning_Numbers__Beginning_2002", "Lottery_Powerball_Winning_Numbers__Beginning_2010", "Motor_Vehicle_Collisions_-_Crashes",
                        "National_Obesity_By_State", "NCHS_-_Death_rates_and_life_expectancy_at_birth", "Popular_Baby_Names", "Real_Estate_Sales_2001-2020_GL", "Traffic_Crashes_" +
                        "-_Crashes", "Warehouse_and_Retail_Sales"};
                this.separator = ',';
                this.hasHeader = true;
                this.fileEnding = ".csv";
            }
            case UEFA -> {
                this.datasetName = "uefa";
                this.relationNames = new String[]{"attacking", "attempts", "defending", "disciplinary", "distributon", "goalkeeping", "goals", "key_stats"};
                this.separator = ',';
                this.hasHeader = true;
                this.fileEnding = ".csv";
            }
            case MUSICBRAINZ -> {
                this.datasetName = "musicbrainz";
                this.relationNames = new String[]{"alternative_release_type", "area", "area_alias", "area_alias_type", "area_gid_redirect", "area_type", "artist", "artist_alias",
                        "artist_alias_type", "artist_credit", "artist_credit_gid_redirect", "artist_credit_name", "artist_gid_redirect", "artist_ipi", "artist_isni",
                        "artist_type", "cdtoc", "country_area", "editor_collection_type", "event", "event_alias", "event_alias_type", "event_gid_redirect", "event_type", "gender"
                        , "genre", "genre_alias", "genre_alias_type", "instrument", "instrument_alias", "instrument_alias_type", "instrument_gid_redirect", "instrument_type",
                        "iso_3166_1", "iso_3166_2", "iso_3166_3", "isrc", "iswc", "label", "label_alias", "label_alias_type", "label_gid_redirect", "label_ipi", "label_isni",
                        "label_type", "language", "link", "link_attribute", "link_attribute_credit", "link_attribute_text_value", "link_attribute_type",
                        "link_creditable_attribute_type", "link_text_attribute_type", "link_type", "link_type_attribute_type", "l_area_area", "l_area_event", "l_area_genre",
                        "l_area_instrument", "l_area_label", "l_area_recording", "l_area_release", "l_area_series", "l_area_url", "l_area_work", "l_artist_artist",
                        "l_artist_event", "l_artist_instrument", "l_artist_label", "l_artist_place", "l_artist_recording", "l_artist_release", "l_artist_release_group",
                        "l_artist_series", "l_artist_url", "l_artist_work", "l_event_event", "l_event_label", "l_event_place", "l_event_recording", "l_event_release",
                        "l_event_release_group", "l_event_series", "l_event_url", "l_event_work", "l_genre_genre", "l_genre_instrument", "l_genre_url", "l_instrument_instrument"
                        , "l_instrument_label", "l_instrument_url", "l_label_label", "l_label_place", "l_label_recording", "l_label_release", "l_label_release_group",
                        "l_label_series", "l_label_url", "l_label_work", "l_place_place", "l_place_recording", "l_place_release", "l_place_series", "l_place_url", "l_place_work"
                        , "l_recording_recording", "l_recording_release", "l_recording_series", "l_recording_url", "l_recording_work", "l_release_group_release_group",
                        "l_release_group_series", "l_release_group_url", "l_release_release", "l_release_series", "l_release_url", "l_series_series", "l_series_url",
                        "l_series_work", "l_url_work", "l_work_work", "medium", "medium_cdtoc", "medium_format", "orderable_link_type", "place", "place_alias", "place_alias_type"
                        , "place_gid_redirect", "place_type", "recording", "recording_alias", "recording_alias_type", "recording_gid_redirect", "release", "release_alias",
                        "release_alias_type", "release_country", "release_gid_redirect", "release_group", "release_group_alias", "release_group_alias_type",
                        "release_group_gid_redirect", "release_group_primary_type", "release_group_secondary_type", "release_group_secondary_type_join", "release_label",
                        "release_packaging", "release_status", "release_unknown_country", "replication_control", "script", "series", "series_alias", "series_alias_type",
                        "series_gid_redirect", "series_ordering_type", "series_type", "track", "track_gid_redirect", "url", "url_gid_redirect", "work", "work_alias",
                        "work_alias_type", "work_attribute", "work_attribute_type", "work_attribute_type_allowed_value", "work_gid_redirect", "work_language", "work_type"};
                this.separator = '\t';
                this.hasHeader = false;
                this.fileEnding = "";
                this.quoteChar = '\0';
                this.nullString = "\\N";
            }
            default -> {
            }
        }
    }

    public enum Dataset {
        TPCH_1, KAGGLE, DATA_GOV, UEFA, TEST, ANIMAL_CROSSING, MUSICBRAINZ
    }

    public enum NullHandling {
        SUBSET, FOREIGN, EQUALITY, INEQUALITY
    }

    public enum DuplicateHandling {
        AWARE, UNAWARE
    }
}
