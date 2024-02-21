package binder.core;

import binder.io.DefaultFileInputGenerator;
import binder.io.FileInputIterator;
import binder.runner.Config;
import binder.structures.AttributeCombination;
import binder.structures.pINDSingleLinkedList;
import binder.utils.CollectionUtils;
import binder.utils.FileUtils;
import binder.utils.PrintUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;

public class BINDER {

    public DefaultFileInputGenerator[] fileInputGenerator = null;
    public String[] tableNames = null;
    public long[] tableSizes = null;
    public String databaseName = null;
    public boolean cleanTemp = false;
    public boolean detectNary = true;
    public int inputRowLimit = -1;
    public int numBucketsPerColumn = 10; // Initial number of buckets per column
    public int memoryCheckFrequency = 100; // Number of new, i.e., so far unseen values during bucketing that trigger a memory consumption check
    public int maxMemoryUsagePercentage = 60; // The algorithm spills to disc if memory usage exceeds X% of available memory
    public int numColumns;
    public long availableMemory;
    public long maxMemoryUsage;
    public File tempFolder = null;
    public int numUnaryINDs = 0;
    public int numNaryINDs = 0;
    public BitSet nullValueColumns;
    public long unaryStatisticTime = -1;
    public long unaryLoadTime = -1;
    public long unaryCompareTime = -1;
    public LongArrayList naryGenerationTime = null;
    public LongArrayList naryLoadTime = null;
    public LongArrayList naryCompareTime = null;
    public long outputTime = -1;
    public IntArrayList activeAttributesPerBucketLevel;
    public IntArrayList naryActiveAttributesPerBucketLevel;
    public int[] spillCounts = null;
    public List<int[]> narySpillCounts = null;
    public int[] refinements = null;
    public List<int[]> naryRefinements = null;
    public int[] bucketComparisonOrder = null;
    public LongArrayList columnSizes = null;
    public boolean nullIsNull = true;
    protected String tempFolderPath = "BINDER_temp"; // TODO: Use Metanome temp file functionality here (interface TempFileAlgorithm)
    protected boolean nullIsSubset = false;
    protected int maxNaryLevel = -1;
    protected Config config;
    double threshold = 1;
    Int2ObjectOpenHashMap<List<Map<String, Long>>> attribute2subBucketsCache = null;
    int[] tableColumnStartIndexes = null;
    List<String> columnNames = null;
    List<String> columnTypes = null;
    int[] column2table = null;
    Int2ObjectOpenHashMap<pINDSingleLinkedList> dep2ref = null;
    private Map<AttributeCombination, List<AttributeCombination>> naryDep2ref = null;

    @Override
    public String toString() {
        return PrintUtils.toString(this);
    }

    public void execute() throws IOException {
        try {
            this.tableSizes = new long[this.tableNames.length];
            ////////////////////////////////////////////////////////
            // Phase 0: Initialization (Collect basic statistics) //
            ////////////////////////////////////////////////////////
            this.unaryStatisticTime = System.currentTimeMillis();
            Initializer.initialize(this);
            this.unaryStatisticTime = System.currentTimeMillis() - this.unaryStatisticTime;

            //////////////////////////////////////////////////////
            // Phase 1: Bucketing (Create and fill the buckets) //
            //////////////////////////////////////////////////////
            this.unaryLoadTime = System.currentTimeMillis();
            Bucketizer.bucketize(this);
            this.unaryLoadTime = System.currentTimeMillis() - this.unaryLoadTime;

            //////////////////////////////////////////////////////
            // Phase 2: Checking (Check INDs using the buckets) //
            //////////////////////////////////////////////////////
            this.unaryCompareTime = System.currentTimeMillis();
            Validator validator = new Validator(this);
            validator.checkViaTwoStageIndexAndLists();
            this.unaryCompareTime = System.currentTimeMillis() - this.unaryCompareTime;

            /////////////////////////////////////////////////////////
            // Phase 3: N-ary IND detection (Find INDs of size > 1 //
            /////////////////////////////////////////////////////////
            if (this.detectNary)
                this.detectNaryViaBucketing(validator);

            //////////////////////////////////////////////////////
            // Phase 4: Output (Return and/or write the results //
            //////////////////////////////////////////////////////
            this.outputTime = System.currentTimeMillis();
            this.output();
            this.outputTime = System.currentTimeMillis() - this.outputTime;

            System.out.println(this);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        } finally {

            // Clean temp
            if (this.cleanTemp)
                FileUtils.cleanDirectory(this.tempFolder);
        }
    }


    private void detectNaryViaBucketing(Validator validator) throws IOException {
        System.out.print("N-ary IND detection ...");

        // Clean temp
        if (this.cleanTemp)
            FileUtils.cleanDirectory(this.tempFolder);

        // N-ary column combinations are enumerated following the enumeration of the attributes
        int naryOffset = this.numColumns;

        // Initialize counters
        this.naryActiveAttributesPerBucketLevel = new IntArrayList();
        this.narySpillCounts = new ArrayList<>();
        this.naryRefinements = new ArrayList<>();

        // Initialize nPlusOneAryDep2ref with unary dep2ref
        Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<>();
        for (int dep : this.dep2ref.keySet()) {
            AttributeCombination depAttributeCombination = new AttributeCombination(this.column2table[dep], 0L, dep);
            List<AttributeCombination> refAttributeCombinations = new LinkedList<>();

            pINDSingleLinkedList.pINDIterator refIterator = this.dep2ref.get(dep).elementIterator();
            while (refIterator.hasNext()) {
                pINDSingleLinkedList.pINDElement ref = refIterator.next();
                // init with no violations left, will be adjusted at attribute expansion
                refAttributeCombinations.add(new AttributeCombination(this.column2table[ref.referenced], 0, ref.referenced));
            }
            nPlusOneAryDep2ref.put(depAttributeCombination, refAttributeCombinations);
        }

        int naryLevel = 1;

        // Generate, bucketize and test the n-ary INDs level-wise
        this.naryDep2ref = new HashMap<>();
        this.naryGenerationTime = new LongArrayList();
        this.naryLoadTime = new LongArrayList();
        this.naryCompareTime = new LongArrayList();
        while (++naryLevel <= this.maxNaryLevel || this.maxNaryLevel <= 0) {
            System.out.print(" L" + naryLevel);

            // Generate (n+1)-ary IND candidates from the already identified unary and n-ary IND candidates
            final long naryGenerationTimeCurrent = System.currentTimeMillis();

            nPlusOneAryDep2ref = this.generateNPlusOneAryCandidates(nPlusOneAryDep2ref);
            if (nPlusOneAryDep2ref.isEmpty())
                break;

            // Collect all attribute combinations of the current level that are possible refs or deps and enumerate them
            Set<AttributeCombination> attributeCombinationSet = new HashSet<>(nPlusOneAryDep2ref.keySet());
            for (List<AttributeCombination> columnCombination : nPlusOneAryDep2ref.values())
                attributeCombinationSet.addAll(columnCombination);
            List<AttributeCombination> attributeCombinations = new ArrayList<>(attributeCombinationSet);

            // Extend the columnSize array
            for (int i = 0; i < attributeCombinations.size(); i++)
                this.columnSizes.add(0);

            int[] currentNarySpillCounts = new int[attributeCombinations.size()];
            for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
                currentNarySpillCounts[attributeCombinationNumber] = 0;
            this.narySpillCounts.add(currentNarySpillCounts);

            int[] currentNaryRefinements = new int[this.numBucketsPerColumn];
            for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++)
                currentNaryRefinements[bucketNumber] = 0;
            this.naryRefinements.add(currentNaryRefinements);

            this.naryGenerationTime.add(System.currentTimeMillis() - naryGenerationTimeCurrent);

            // Read the input dataset again and bucketize all attribute combinations that are refs or deps
            long naryLoadTimeCurrent = System.currentTimeMillis();
            this.naryBucketize(attributeCombinations, naryOffset, currentNarySpillCounts);
            this.naryLoadTime.add(System.currentTimeMillis() - naryLoadTimeCurrent);

            // Check the n-ary IND candidates
            long naryCompareTimeCurrent = System.currentTimeMillis();
            validator.naryCheckViaTwoStageIndexAndLists(nPlusOneAryDep2ref, attributeCombinations, naryOffset);

            this.naryDep2ref.putAll(nPlusOneAryDep2ref);

            // Add the number of created buckets for n-ary INDs of this level to the naryOffset
            naryOffset = naryOffset + attributeCombinations.size();

            this.naryCompareTime.add(System.currentTimeMillis() - naryCompareTimeCurrent);
            System.out.print("(" + (System.currentTimeMillis() - naryGenerationTimeCurrent) + " ms)");
        }
        System.out.println();
    }

    private Map<AttributeCombination, List<AttributeCombination>> generateNPlusOneAryCandidates(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref) {
        Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<>();

        if ((naryDep2ref == null) || (naryDep2ref.isEmpty()))
            return nPlusOneAryDep2ref;

        int previousSize = naryDep2ref.keySet().iterator().next().size();

        List<AttributeCombination> deps = new ArrayList<>(naryDep2ref.keySet());
        for (int i = 0; i < deps.size() - 1; i++) {
            AttributeCombination depPivot = deps.get(i);
            for (int j = i + 1; j < deps.size(); j++) { // if INDs of the form AA<CD should be discovered as well, remove + 1
                AttributeCombination depExtension = deps.get(j);

                // Ensure same tables
                if (depPivot.getTable() != depExtension.getTable())
                    continue;

                // Ensure same prefix
                if (this.notSamePrefix(depPivot, depExtension))
                    continue;

                int depPivotAttr = depPivot.getAttributes()[previousSize - 1];
                int depExtensionAttr = depExtension.getAttributes()[previousSize - 1];

                // Ensure non-empty attribute extension
                if ((previousSize == 1) && ((this.columnSizes.getLong(depPivotAttr) == 0) || (this.columnSizes.getLong(depExtensionAttr) == 0)))
                    continue;

                for (AttributeCombination refPivot : naryDep2ref.get(depPivot)) {
                    for (AttributeCombination refExtension : naryDep2ref.get(depExtension)) {

                        // Ensure same tables
                        if (refPivot.getTable() != refExtension.getTable())
                            continue;

                        // Ensure same prefix
                        if (this.notSamePrefix(refPivot, refExtension))
                            continue;

                        int refPivotAttr = refPivot.getAttributes()[previousSize - 1];
                        int refExtensionAttr = refExtension.getAttributes()[previousSize - 1];

                        // Ensure that the extension attribute is different from the pivot attribute; remove check if INDs of the form AB<CC should be discovered as well
                        if (refPivotAttr == refExtensionAttr)
                            continue;

                        // We want the lhs and rhs to be disjunctive, because INDs with non-disjunctive sides usually don't have practical relevance; remove this check if INDs with overlapping sides are of interest
                        if ((depPivotAttr == refExtensionAttr) || (depExtensionAttr == refPivotAttr))
                            continue;

                        // Merge the dep attributes and ref attributes, respectively
                        AttributeCombination nPlusOneDep = new AttributeCombination(depPivot.getTable(), (long) ((1 - threshold) * tableSizes[depPivot.getTable()]), depPivot.getAttributes(), depExtensionAttr);
                        AttributeCombination nPlusOneRef = new AttributeCombination(refPivot.getTable(), (long) ((1 - threshold) * tableSizes[refPivot.getTable()]), refPivot.getAttributes(), refExtensionAttr);

                        // Store the new candidate
                        if (!nPlusOneAryDep2ref.containsKey(nPlusOneDep))
                            nPlusOneAryDep2ref.put(nPlusOneDep, new LinkedList<AttributeCombination>());
                        nPlusOneAryDep2ref.get(nPlusOneDep).add(nPlusOneRef);
                    }
                }
            }
        }
        return nPlusOneAryDep2ref;
    }

    private boolean notSamePrefix(AttributeCombination combination1, AttributeCombination combination2) {
        for (int i = 0; i < combination1.size() - 1; i++)
            if (combination1.getAttributes()[i] != combination2.getAttributes()[i])
                return true;
        return false;
    }

    private void naryBucketize(List<AttributeCombination> attributeCombinations, int naryOffset, int[] narySpillCounts) throws IOException {
        // Identify the relevant attribute combinations for the different tables
        List<IntArrayList> table2attributeCombinationNumbers = new ArrayList<>(this.tableNames.length);
        for (int tableNumber = 0; tableNumber < this.tableNames.length; tableNumber++)
            table2attributeCombinationNumbers.add(new IntArrayList());
        for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
            table2attributeCombinationNumbers.get(attributeCombinations.get(attributeCombinationNumber).getTable()).add(attributeCombinationNumber);

        // Count the empty buckets per attribute to identify sparse buckets and promising bucket levels for comparison
        int[] emptyBuckets = new int[this.numBucketsPerColumn];
        for (int levelNumber = 0; levelNumber < this.numBucketsPerColumn; levelNumber++)
            emptyBuckets[levelNumber] = 0;

        for (int tableIndex = 0; tableIndex < this.tableNames.length; tableIndex++) {
            int numTableAttributeCombinations = table2attributeCombinationNumbers.get(tableIndex).size();
            int startTableColumnIndex = this.tableColumnStartIndexes[tableIndex];

            if (numTableAttributeCombinations == 0)
                continue;

            // Initialize buckets
            Int2ObjectOpenHashMap<List<Map<String, Long>>> buckets = new Int2ObjectOpenHashMap<>(numTableAttributeCombinations);
            for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                List<Map<String, Long>> attributeCombinationBuckets = new ArrayList<>();
                for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++)
                    attributeCombinationBuckets.add(new HashMap<String, Long>());
                buckets.put(attributeCombinationNumber, attributeCombinationBuckets);
            }

            // Initialize value counters
            int numValuesSinceLastMemoryCheck = 0;
            int[] numValuesInAttributeCombination = new int[attributeCombinations.size()];
            for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
                numValuesInAttributeCombination[attributeCombinationNumber] = 0;

            // Load data
            FileInputIterator inputIterator = null;
            try {
                inputIterator = new FileInputIterator(this.tableNames[tableIndex], this.config, this.inputRowLimit);

                while (inputIterator.next()) {
                    List<String> values = inputIterator.getValues();

                    for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                        AttributeCombination attributeCombination = attributeCombinations.get(attributeCombinationNumber);

                        // TODO: Why does this skip combinations where any value is NULL?
                        boolean anyNull = false;
                        List<String> attributeCombinationValues = new ArrayList<>(attributeCombination.getAttributes().length);
                        for (int attribute : attributeCombination.getAttributes()) {
                            String attributeValue = values.get(attribute - startTableColumnIndex);
                            anyNull = (attributeValue == null);
                            if (anyNull) break;
                            attributeCombinationValues.add(attributeValue);
                        }
                        if (anyNull) continue;

                        // TODO: This can produce incorrect results in a partial setting
                        String valueSeparator = "#";
                        String value = CollectionUtils.concat(attributeCombinationValues, valueSeparator);

                        // Bucketize
                        int bucketNumber = Bucketizer.calculateBucketFor(value, this.numBucketsPerColumn);
                        long amount = buckets.get(attributeCombinationNumber).get(bucketNumber).getOrDefault(value, 0L);
                        if (amount == 0) {
                            numValuesSinceLastMemoryCheck++;
                            numValuesInAttributeCombination[attributeCombinationNumber] = numValuesInAttributeCombination[attributeCombinationNumber] + 1;
                        }
                        buckets.get(attributeCombinationNumber).get(bucketNumber).put(value, ++amount);

                        // Occasionally check the memory consumption
                        if (numValuesSinceLastMemoryCheck >= this.memoryCheckFrequency) {
                            numValuesSinceLastMemoryCheck = 0;

                            // Spill to disk if necessary
                            while (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > this.maxMemoryUsage) {
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
                                for (int largeBucketNumber = 0; largeBucketNumber < this.numBucketsPerColumn; largeBucketNumber++) {
                                    Bucketizer.writeBucket(this.tempFolder, naryOffset + largestAttributeCombinationNumber, largeBucketNumber, -1, buckets.get(largestAttributeCombinationNumber).get(largeBucketNumber), this.columnSizes);
                                    buckets.get(largestAttributeCombinationNumber).set(largeBucketNumber, new HashMap<>());
                                }

                                numValuesInAttributeCombination[largestAttributeCombinationNumber] = 0;

                                narySpillCounts[largestAttributeCombinationNumber] = narySpillCounts[largestAttributeCombinationNumber] + 1;

                                System.gc();
                            }
                        }
                    }
                }
            } finally {
                inputIterator.close();
            }

            // Write buckets to disk
            for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                if (narySpillCounts[attributeCombinationNumber] == 0) { // if an attribute combination was spilled to disk, we do not count empty buckets for this attribute combination, because the partitioning distributes the values evenly and hence all buckets should have been populated
                    for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
                        Map<String, Long> bucket = buckets.get(attributeCombinationNumber).get(bucketNumber);
                        if (bucket.size() != 0)
                            Bucketizer.writeBucket(this.tempFolder, naryOffset + attributeCombinationNumber, bucketNumber, -1, bucket, this.columnSizes);
                        else
                            emptyBuckets[bucketNumber] = emptyBuckets[bucketNumber] + 1;
                    }
                } else {
                    for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
                        Map<String, Long> bucket = buckets.get(attributeCombinationNumber).get(bucketNumber);
                        if (bucket.size() != 0)
                            Bucketizer.writeBucket(this.tempFolder, naryOffset + attributeCombinationNumber, bucketNumber, -1, bucket, this.columnSizes);
                    }
                }
            }
        }

        // Calculate the bucket comparison order from the emptyBuckets to minimize the influence of sparse-attribute-issue
        Bucketizer.calculateBucketComparisonOrder(emptyBuckets, this.numBucketsPerColumn, this.numColumns, this);
    }

    private void output() {
        System.out.println("Generating output ...");

        // Output unary INDs
        for (int dep : this.dep2ref.keySet()) {
            String depTableName = this.getTableNameFor(dep, this.tableColumnStartIndexes);
            String depColumnName = this.columnNames.get(dep);

            pINDSingleLinkedList.pINDIterator refIterator = this.dep2ref.get(dep).elementIterator();
            while (refIterator.hasNext()) {
                pINDSingleLinkedList.pINDElement ref = refIterator.next();

                String refTableName = this.getTableNameFor(ref.referenced, this.tableColumnStartIndexes);
                String refColumnName = this.columnNames.get(ref.referenced);

                // TODO: figure out how to store pINDs
                this.numUnaryINDs++;
            }
        }

        // Output n-ary INDs
        if (this.naryDep2ref == null)
            return;
        for (AttributeCombination depAttributeCombination : this.naryDep2ref.keySet()) {

            for (AttributeCombination refAttributeCombination : this.naryDep2ref.get(depAttributeCombination)) {

                // TODO: figure out how to store pINDs
                this.numNaryINDs++;
            }
        }
    }

    private String getTableNameFor(int column, int[] tableColumnStartIndexes) {
        for (int i = 1; i < tableColumnStartIndexes.length; i++)
            if (tableColumnStartIndexes[i] > column)
                return this.tableNames[i - 1];
        return this.tableNames[this.tableNames.length - 1];
    }
}
