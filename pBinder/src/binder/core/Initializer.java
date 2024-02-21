package binder.core;

import binder.io.DefaultFileInputGenerator;
import binder.io.RelationalFileInput;
import binder.utils.FileUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.BitSet;

public class Initializer {
    static Logger logger = LoggerFactory.getLogger(Initializer.class);

    public static void initialize(BINDER binder) throws IOException {

        logger.info("Starting initialization");

        // Ensure the presence of an input generator
        if (binder.fileInputGenerator == null)
            return;

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
        logger.info("Finished initialization");
    }

    /**
     * This method prepare a bunch of Metadata by reading each tables header.
     *
     * @param binder The Algorithm class
     */
    private static void initializeMetaData(BINDER binder) throws IOException {

        logger.info("Collecting Metadata");
        // initialize empty variables
        binder.tableColumnStartIndexes = new int[binder.tableNames.length];
        binder.columnNames = new ArrayList<>();
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

    static void collectStatisticsFrom(BINDER binder, DefaultFileInputGenerator inputGenerator) throws IOException {
        RelationalFileInput input = inputGenerator.generateNewCopy();
        // Query attribute names and types
        binder.columnNames.addAll(input.headerLine);
        input.close();
    }

}
