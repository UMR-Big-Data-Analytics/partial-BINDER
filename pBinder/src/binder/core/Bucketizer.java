package binder.core;

import binder.io.FileInputIterator;
import binder.structures.AttributeCombination;
import binder.structures.Level;
import binder.utils.CollectionUtils;
import binder.utils.FileUtils;
import binder.utils.MeasurementUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;

public class Bucketizer {
    static Logger logger = LoggerFactory.getLogger(Bucketizer.class);

    /**
     * Unary Bucketizing
     *
     * @param binder The BINDER object which should be bucketized
     * @throws IOException if something goes wrong during file handling
     */
    public static void unaryBucketize(BINDER binder) throws IOException {

        for (int tableIndex = 0; tableIndex < binder.tableNames.length; tableIndex++) {
            String tableName = binder.tableNames[tableIndex];

            // get the index where the columns start
            int startTableColumnIndex = binder.tableColumnStartIndexes[tableIndex];
            // get the number of columns belonging to the given table
            int numTableColumns = (binder.tableColumnStartIndexes.length > tableIndex + 1) ? binder.tableColumnStartIndexes[tableIndex + 1] - startTableColumnIndex : binder.numColumns - startTableColumnIndex;

            logger.info("(" + (tableIndex + 1) + "/" + (binder.tableNames.length) + ") Building unary buckets for " + tableName + " [" + numTableColumns + "]");

            // Initialize buckets
            List<List<Map<String, Long>>> buckets = initializeBuckets(binder, numTableColumns);

            // Initialize value counters
            int numValuesSinceLastMemoryCheck = 0;
            int[] numValuesInColumn = new int[binder.numColumns];

            // Load data for the current table
            FileInputIterator inputIterator = null;
            try {
                inputIterator = new FileInputIterator(tableName, binder.config, binder.inputRowLimit);
                long rowCount = 0;

                while (inputIterator.next()) {
                    rowCount++;
                    for (int columnNumber = 0; columnNumber < numTableColumns; columnNumber++) {
                        String value = inputIterator.getValue(columnNumber);

                        if (value == null) {
                            binder.nullValueColumns.set(startTableColumnIndex + columnNumber);
                            continue;
                        }
                        // Bucketize
                        int bucketNumber = calculateBucketFor(value, binder.numBucketsPerColumn);
                        long amount = buckets.get(columnNumber).get(bucketNumber).getOrDefault(value, 0L);
                        if (amount == 0) {
                            numValuesSinceLastMemoryCheck++;
                            numValuesInColumn[columnNumber] = numValuesInColumn[columnNumber] + 1;
                        }
                        buckets.get(columnNumber).get(bucketNumber).put(value, ++amount);

                        // Occasionally check the memory consumption
                        if (numValuesSinceLastMemoryCheck >= binder.memoryCheckFrequency) {
                            numValuesSinceLastMemoryCheck = 0;

                            spillTillMemoryUnderThreshold(binder, numTableColumns, startTableColumnIndex, buckets, numValuesInColumn);
                        }
                    }
                }
                binder.tableSizes[tableIndex] = rowCount;
            } finally {
                if (inputIterator != null) inputIterator.close();
            }

            // Write buckets to disk
            toDisk(binder, numTableColumns, startTableColumnIndex, buckets);
        }

        // Calculate the bucket comparison order from the emptyBuckets to minimize the influence of sparse-attribute-issue
        calculateBucketComparisonOrder(binder.numBucketsPerColumn, binder.numColumns, binder);
    }

    /**
     * Given the relevant attribute combinations, this method generates all required buckets.
     *
     * @param binder                the binder algorithm
     * @param attributeCombinations all relevant attribute combinations. No matter if referred or dependant
     * @param naryOffset            the number of attributes created in lower levels. Used as a bucket id system
     * @param narySpillCounts       logging array
     * @throws IOException If something goes wrong during bucket reading/writing
     */
    static void naryBucketize(BINDER binder, List<AttributeCombination> attributeCombinations, int naryOffset, int[] narySpillCounts) throws IOException {
        // Identify the relevant attribute combinations for the different tables
        List<IntArrayList> table2attributeCombinationNumbers = new ArrayList<>(binder.tableNames.length);
        for (int tableNumber = 0; tableNumber < binder.tableNames.length; tableNumber++)
            table2attributeCombinationNumbers.add(new IntArrayList());
        for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
            table2attributeCombinationNumbers.get(attributeCombinations.get(attributeCombinationNumber).getTable()).add(attributeCombinationNumber);

        for (int tableIndex = 0; tableIndex < binder.tableNames.length; tableIndex++) {
            int numTableAttributeCombinations = table2attributeCombinationNumbers.get(tableIndex).size();
            int startTableColumnIndex = binder.tableColumnStartIndexes[tableIndex];

            logger.info("(" + (tableIndex + 1) + "/" + binder.tableNames.length + ") Building nary buckets for " + binder.tableNames[tableIndex] + " [" + numTableAttributeCombinations + "]");

            if (numTableAttributeCombinations == 0) {
                logger.debug("Skipped table " + binder.tableNames[tableIndex]);
                continue;
            }

            // Initialize buckets
            Int2ObjectOpenHashMap<List<Map<String, Long>>> buckets = new Int2ObjectOpenHashMap<>(numTableAttributeCombinations);
            for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                List<Map<String, Long>> attributeCombinationBuckets = new ArrayList<>();
                for (int bucketNumber = 0; bucketNumber < binder.numBucketsPerColumn; bucketNumber++)
                    attributeCombinationBuckets.add(new HashMap<>());
                buckets.put(attributeCombinationNumber, attributeCombinationBuckets);
            }

            // Initialize value counters
            int numValuesSinceLastMemoryCheck = 0;
            int[] numValuesInAttributeCombination = new int[attributeCombinations.size()];
            for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
                numValuesInAttributeCombination[attributeCombinationNumber] = 0;

            // Load data
            FileInputIterator inputIterator = new FileInputIterator(binder.tableNames[tableIndex], binder.config, binder.inputRowLimit);

            while (inputIterator.next()) {
                List<String> values = inputIterator.getValues();

                for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                    AttributeCombination attributeCombination = attributeCombinations.get(attributeCombinationNumber);

                    boolean anyNull = false;
                    List<String> attributeCombinationValues = new ArrayList<>(attributeCombination.getAttributes().length);
                    for (int attribute : attributeCombination.getAttributes()) {
                        String attributeValue = values.get(attribute - startTableColumnIndex);
                        anyNull = (attributeValue == null);
                        if (anyNull) break;
                        attributeCombinationValues.add(attributeValue);
                    }
                    if (anyNull) {
                        attributeCombination.nulls++;
                    }

                    String valueSeparator = "#";
                    String value = CollectionUtils.concat(attributeCombinationValues, valueSeparator);

                    // Bucketize
                    int bucketNumber = Bucketizer.calculateBucketFor(value, binder.numBucketsPerColumn);
                    long amount = buckets.get(attributeCombinationNumber).get(bucketNumber).getOrDefault(value, 0L);
                    if (amount == 0) {
                        numValuesSinceLastMemoryCheck++;
                        numValuesInAttributeCombination[attributeCombinationNumber] = numValuesInAttributeCombination[attributeCombinationNumber] + 1;
                    }
                    buckets.get(attributeCombinationNumber).get(bucketNumber).put(value, ++amount);

                    // Occasionally check the memory consumption
                    if (numValuesSinceLastMemoryCheck >= binder.memoryCheckFrequency) {
                        numValuesSinceLastMemoryCheck = 0;

                        // Spill to disk if necessary
                        while (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > binder.maxMemoryUsage) {
                            // Identify largest buffer
                            int largestAttributeCombinationNumber = 0;
                            int largestAttributeCombinationSize = numValuesInAttributeCombination[largestAttributeCombinationNumber];
                            for (int otherAttributeCombinationNumber = 1; otherAttributeCombinationNumber < numValuesInAttributeCombination.length; otherAttributeCombinationNumber++) {
                                if (largestAttributeCombinationSize < numValuesInAttributeCombination[otherAttributeCombinationNumber]) {
                                    largestAttributeCombinationNumber = otherAttributeCombinationNumber;
                                    largestAttributeCombinationSize = numValuesInAttributeCombination[otherAttributeCombinationNumber];
                                }
                            }

                            // Write buckets from the largest column to disk and empty written buckets
                            for (int largeBucketNumber = 0; largeBucketNumber < binder.numBucketsPerColumn; largeBucketNumber++) {
                                writeBucket(binder.tempFolder, naryOffset + largestAttributeCombinationNumber, largeBucketNumber, -1, buckets.get(largestAttributeCombinationNumber).get(largeBucketNumber), binder.columnSizes);
                                buckets.get(largestAttributeCombinationNumber).set(largeBucketNumber, new HashMap<>());
                            }

                            numValuesInAttributeCombination[largestAttributeCombinationNumber] = 0;

                            narySpillCounts[largestAttributeCombinationNumber] = narySpillCounts[largestAttributeCombinationNumber] + 1;

                            System.gc();
                        }
                    }
                }
            }
            inputIterator.close();


            // Write buckets to disk
            for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                for (int bucketNumber = 0; bucketNumber < binder.numBucketsPerColumn; bucketNumber++) {
                    Map<String, Long> bucket = buckets.get(attributeCombinationNumber).get(bucketNumber);
                    if (bucket.size() != 0)
                        Bucketizer.writeBucket(binder.tempFolder, naryOffset + attributeCombinationNumber, bucketNumber, -1, bucket, binder.columnSizes);
                }
            }
        }

        // Calculate the bucket comparison order from the emptyBuckets to minimize the influence of sparse-attribute-issue
        calculateBucketComparisonOrder(binder.numBucketsPerColumn, binder.numColumns, binder);
    }


    private static List<List<Map<String, Long>>> initializeBuckets(BINDER binder, int numTableColumns) {
        List<List<Map<String, Long>>> buckets = new ArrayList<>(numTableColumns);
        for (int columnNumber = 0; columnNumber < numTableColumns; columnNumber++) {
            List<Map<String, Long>> attributeBuckets = new ArrayList<>();
            for (int bucketNumber = 0; bucketNumber < binder.numBucketsPerColumn; bucketNumber++)
                attributeBuckets.add(new HashMap<>());
            buckets.add(attributeBuckets);
        }
        return buckets;
    }

    private static void spillTillMemoryUnderThreshold(BINDER binder, int numTableColumns, int startTableColumnIndex, List<List<Map<String, Long>>> buckets, int[] numValuesInColumn) throws IOException {
        // Spill to disk if necessary
        while (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > binder.maxMemoryUsage) {
            // Identify largest buffer
            int largestColumnNumber = 0;
            int largestColumnSize = numValuesInColumn[largestColumnNumber];
            for (int otherColumnNumber = 1; otherColumnNumber < numTableColumns; otherColumnNumber++) {
                if (largestColumnSize < numValuesInColumn[otherColumnNumber]) {
                    largestColumnNumber = otherColumnNumber;
                    largestColumnSize = numValuesInColumn[otherColumnNumber];
                }
            }

            // Write buckets from the largest column to disk and empty written buckets
            int globalLargestColumnIndex = startTableColumnIndex + largestColumnNumber;
            for (int largeBucketNumber = 0; largeBucketNumber < binder.numBucketsPerColumn; largeBucketNumber++) {
                writeBucket(binder.tempFolder, globalLargestColumnIndex, largeBucketNumber, -1, buckets.get(largestColumnNumber).get(largeBucketNumber), binder.columnSizes);
                buckets.get(largestColumnNumber).set(largeBucketNumber, new HashMap<>());
            }
            numValuesInColumn[largestColumnNumber] = 0;

            binder.spillCounts[globalLargestColumnIndex] = binder.spillCounts[globalLargestColumnIndex] + 1;

            System.gc();
        }
    }

    private static void toDisk(BINDER binder, int numTableColumns, int startTableColumnIndex, List<List<Map<String, Long>>> buckets) throws IOException {
        for (int columnNumber = 0; columnNumber < numTableColumns; columnNumber++) {
            int globalColumnIndex = startTableColumnIndex + columnNumber;
            for (int bucketNumber = 0; bucketNumber < binder.numBucketsPerColumn; bucketNumber++) {
                Map<String, Long> bucket = buckets.get(columnNumber).get(bucketNumber);
                if (bucket.size() != 0)
                    writeBucket(binder.tempFolder, globalColumnIndex, bucketNumber, -1, bucket, binder.columnSizes);
            }
        }
    }

    static int calculateBucketFor(String value, int numBucketsPerColumn) {
        return Math.abs(value.hashCode() % numBucketsPerColumn); // range partitioning
    }

    private static int calculateBucketFor(String value, int bucketNumber, int numSubBuckets, int numBucketsPerColumn) {
        return ((Math.abs(value.hashCode() % (numBucketsPerColumn * numSubBuckets)) - bucketNumber) / numBucketsPerColumn); // range partitioning
    }

    static void calculateBucketComparisonOrder(int numBucketsPerColumn, int numColumns, BINDER binder) {
        List<Level> levels = new ArrayList<>(numColumns);
        for (int level = 0; level < numBucketsPerColumn; level++)
            levels.add(new Level(level, 0));
        Collections.sort(levels);

        binder.bucketComparisonOrder = new int[numBucketsPerColumn];
        for (int rank = 0; rank < numBucketsPerColumn; rank++)
            binder.bucketComparisonOrder[rank] = levels.get(rank).number();
    }

    static void writeBucket(File tempFolder, int attributeNumber, int bucketNumber, int subBucketNumber, Map<String, Long> values, ArrayList<Long> columnSizes) throws IOException {
        // Write the values
        String bucketFilePath = getBucketFilePath(tempFolder, attributeNumber, bucketNumber, subBucketNumber);
        writeToDisk(bucketFilePath, values);

        // Add the size of the written values to the size of the current attribute
        long size = columnSizes.get(attributeNumber);
        // Bytes that each value requires in the comparison phase for the indexes
        int overheadPerValueForIndexes = 64;
        for (String value : values.keySet())
            size = size + MeasurementUtils.sizeOf64(value) + overheadPerValueForIndexes;
        columnSizes.set(attributeNumber, size);
    }

    private static void writeToDisk(String bucketFilePath, Map<String, Long> values) throws IOException {
        if ((values == null) || (values.isEmpty())) return;

        BufferedWriter writer = null;
        try {
            writer = FileUtils.buildFileWriter(bucketFilePath, true);
            for (String value : values.keySet()) {
                writer.write(value);
                writer.newLine();
                writer.write(values.get(value).toString());
                writer.newLine();
            }
            writer.flush();
        } finally {
            FileUtils.close(writer);
        }
    }

    static Map<String, Long> readBucketAsList(BINDER binder, int attributeNumber, int bucketNumber, int subBucketNumber) throws IOException {
        if ((binder.attribute2subBucketsCache != null) && (binder.attribute2subBucketsCache.containsKey(attributeNumber)))
            return binder.attribute2subBucketsCache.get(attributeNumber).get(subBucketNumber);

        Map<String, Long> bucket = new HashMap<>(); // Reading buckets into Lists keeps duplicates within these buckets
        String bucketFilePath = getBucketFilePath(binder.tempFolder, attributeNumber, bucketNumber, subBucketNumber);
        readFromDisk(bucketFilePath, bucket);
        return bucket;
    }

    private static void readFromDisk(String bucketFilePath, Map<String, Long> values) throws IOException {
        File file = new File(bucketFilePath);
        if (!file.exists()) return;

        BufferedReader reader = null;
        String value;
        try {
            reader = FileUtils.buildFileReader(bucketFilePath);
            while ((value = reader.readLine()) != null) {
                long amount = Long.parseLong(reader.readLine());
                values.put(value, amount);
            }
        } finally {
            FileUtils.close(reader);
        }
    }

    private static BufferedReader getBucketReader(File tempFolder, int attributeNumber, int bucketNumber) throws IOException {
        String bucketFilePath = getBucketFilePath(tempFolder, attributeNumber, bucketNumber, -1);

        File file = new File(bucketFilePath);
        if (!file.exists()) return null;

        return FileUtils.buildFileReader(bucketFilePath);
    }

    private static String getBucketFilePath(File tempFolder, int attributeNumber, int bucketNumber, int subBucketNumber) {
        if (subBucketNumber >= 0)
            return tempFolder.getPath() + File.separator + attributeNumber + File.separator + bucketNumber + "_" + subBucketNumber;
        return tempFolder.getPath() + File.separator + attributeNumber + File.separator + bucketNumber;
    }

    static int[] refineBucketLevel(BINDER binder, BitSet activeAttributes, int attributeOffset, int level) throws IOException {
        // The offset is used for n-ary INDs, because their buckets are placed behind the unary buckets on disk, which is important if the unary buckets have not been deleted before
        // Empty sub bucket cache, because it will be refilled in the following
        binder.attribute2subBucketsCache = null;

        // Give a hint to the gc
        System.gc();

        // Measure the size of the level and find the attribute with the largest bucket
        int numAttributes = 0;
        long levelSize = 0;
        for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
            numAttributes++;
            int attributeIndex = attribute + attributeOffset;
            long bucketSize = binder.columnSizes.get(attributeIndex) / binder.numBucketsPerColumn;
            levelSize = levelSize + bucketSize;
        }

        // If there are no active attributes, no refinement is needed
        if (numAttributes == 0) {
            int[] subBucketNumbers = new int[1];
            subBucketNumbers[0] = -1;
            return subBucketNumbers;
        }

        // Define the number of sub buckets
        long maxBucketSize = binder.maxMemoryUsage / numAttributes;
        int numSubBuckets = (int) (levelSize / binder.maxMemoryUsage) + 1;

        int[] subBucketNumbers = new int[numSubBuckets];

        // If the current level fits into memory, no refinement is needed
        if (numSubBuckets == 1) {
            subBucketNumbers[0] = -1;
            return subBucketNumbers;
        }

        for (int subBucketNumber = 0; subBucketNumber < numSubBuckets; subBucketNumber++)
            subBucketNumbers[subBucketNumber] = subBucketNumber;

        if (attributeOffset == 0) binder.refinements[level] = numSubBuckets;
        else binder.naryRefinements.get(binder.naryRefinements.size() - 1)[level] = numSubBuckets;

        binder.attribute2subBucketsCache = new Int2ObjectOpenHashMap<>(numSubBuckets);

        // Refine
        for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
            int attributeIndex = attribute + attributeOffset;

            List<Map<String, Long>> subBuckets = new ArrayList<>(numSubBuckets);
            for (int subBucket = 0; subBucket < numSubBuckets; subBucket++)
                subBuckets.add(new HashMap<>());

            BufferedReader reader = null;
            String value;
            boolean spilled = false;
            try {
                reader = getBucketReader(binder.tempFolder, attributeIndex, level);

                if (reader != null) {
                    int numValuesSinceLastMemoryCheck = 0;

                    while ((value = reader.readLine()) != null) {
                        int bucketNumber = calculateBucketFor(value, level, numSubBuckets, binder.numBucketsPerColumn);
                        long amount = Long.parseLong(reader.readLine());
                        subBuckets.get(bucketNumber).put(value, amount);
                        numValuesSinceLastMemoryCheck++;

                        // Occasionally check the memory consumption
                        if (numValuesSinceLastMemoryCheck >= binder.memoryCheckFrequency) {
                            numValuesSinceLastMemoryCheck = 0;

                            // Spill to disk if necessary
                            if (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > binder.maxMemoryUsage) {
                                for (int subBucket = 0; subBucket < numSubBuckets; subBucket++) {
                                    writeBucket(binder.tempFolder, attributeIndex, level, subBucket, subBuckets.get(subBucket), binder.columnSizes);
                                    subBuckets.set(subBucket, new HashMap<>());
                                }

                                spilled = true;
                                System.gc();
                            }
                        }
                    }
                }
            } finally {
                FileUtils.close(reader);
            }

            // Large sub bucketings need to be written to disk; small sub bucketings can stay in memory
            if ((binder.columnSizes.get(attributeIndex) / binder.numBucketsPerColumn > maxBucketSize) || spilled)
                for (int subBucket = 0; subBucket < numSubBuckets; subBucket++)
                    writeBucket(binder.tempFolder, attributeIndex, level, subBucket, subBuckets.get(subBucket), binder.columnSizes);
            else binder.attribute2subBucketsCache.put(attributeIndex, subBuckets);
        }

        return subBucketNumbers;
    }


}
