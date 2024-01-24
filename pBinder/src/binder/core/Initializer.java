package binder.core;

import binder.io.RelationalFileInput;
import binder.utils.FileUtils;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;

public class Initializer {

    public static void initialize(BINDER binder) throws InputGenerationException, SQLException, AlgorithmConfigurationException {
        System.out.println("Initializing ...");

        // Ensure the presence of an input generator
        if (binder.fileInputGenerator == null)
            throw new InputGenerationException("No input generator specified!");

        // Initialize temp folder
        binder.tempFolder = new File(binder.tempFolderPath + File.separator + "temp");

        // Clean temp if there are files from previous runs that may pollute this run
        FileUtils.cleanDirectory(binder.tempFolder);

        // Initialize memory management
        binder.availableMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        binder.maxMemoryUsage = (long) (binder.availableMemory * (binder.maxMemoryUsagePercentage / 100.0f));

        // Query meta data for input tables
        initializeMetaData(binder);

        // Build an index that assigns the columns to their tables, because the n-ary detection can only group those attributes that belong to the same table and the foreign key detection also only groups attributes from different tables.
        binder.column2table = new int[binder.numColumns];
        int table = 0;
        for (int i = 0; i < binder.tableColumnStartIndexes.length; i++) {
            int currentStart = binder.tableColumnStartIndexes[i];
            int nextStart = ((i + 1) == binder.tableColumnStartIndexes.length) ? binder.numColumns : binder.tableColumnStartIndexes[i + 1];

            for (int j = currentStart; j < nextStart; j++)
                binder.column2table[j] = table;
            table++;
        }
    }

    /**
     * This method prepare a bunch of Metadata by reading each tables header.
     * @param binder The Algorithm class
     * @throws InputGenerationException If no file was found
     * @throws AlgorithmConfigurationException If the configuration is incorrect
     */
    private static void initializeMetaData(BINDER binder) throws InputGenerationException, AlgorithmConfigurationException {

        // initialize empty variables
        binder.tableColumnStartIndexes = new int[binder.tableNames.length];
        binder.columnNames = new ArrayList<>();
        binder.columnTypes = new ArrayList<>();
        binder.activeAttributesPerBucketLevel = new IntArrayList(binder.numBucketsPerColumn);
        binder.refinements = new int[binder.numBucketsPerColumn];

        for (int tableIndex = 0; tableIndex < binder.tableNames.length; tableIndex++) {
            // remember with columns belong to which table
            binder.tableColumnStartIndexes[tableIndex] = binder.columnNames.size();

            // Fill the lists' column Names and columnTypes
            collectStatisticsFrom(binder, binder.fileInputGenerator[tableIndex]);
        }

        // update the pointer to respect the new columns
        binder.numColumns = binder.columnNames.size();

        binder.nullValueColumns = new BitSet(binder.numColumns);

        binder.spillCounts = new int[binder.numColumns];
        for (int columnNumber = 0; columnNumber < binder.numColumns; columnNumber++)
            binder.spillCounts[columnNumber] = 0;

        for (int bucketNumber = 0; bucketNumber < binder.numBucketsPerColumn; bucketNumber++)
            binder.refinements[bucketNumber] = 0;

    }

    static void collectStatisticsFrom(BINDER binder, RelationalInputGenerator inputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        RelationalFileInput input = null;
        try {
            // Query attribute names and types
            input = (RelationalFileInput) inputGenerator.generateNewCopy();
            for (String columnName : input.headerLine) {
                binder.columnNames.add(columnName);
                binder.columnTypes.add("String");
            }
        } finally {
            FileUtils.close(input);
        }
    }

}
