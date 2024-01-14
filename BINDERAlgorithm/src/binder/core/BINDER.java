package binder.core;

import binder.io.FileInputIterator;
import binder.io.InputIterator;
import binder.structures.AttributeCombination;
import binder.utils.*;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithms.binder.structures.IntSingleLinkedList;
import de.metanome.algorithms.binder.structures.IntSingleLinkedList.ElementIterator;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.lucene.util.OpenBitSet;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.*;

// Bucketing IND ExtractoR (BINDER)
public class BINDER {

    public RelationalInputGenerator[] fileInputGenerator = null;
    public InclusionDependencyResultReceiver resultReceiver = null;
    public DataAccessObject dao = null;
    public String[] tableNames = null;
    public String databaseName = null;
    public boolean cleanTemp = true;
    public boolean detectNary = false;
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
    public OpenBitSet nullValueColumns;
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
    protected String tempFolderPath = "BINDER_temp"; // TODO: Use Metanome temp file functionality here (interface TempFileAlgorithm)
    protected boolean filterKeyForeignkeys = false;
    protected int maxNaryLevel = -1;
    Int2ObjectOpenHashMap<List<List<String>>> attribute2subBucketsCache = null;
    int[] tableColumnStartIndexes = null;
    List<String> columnNames = null;
    List<String> columnTypes = null;
    int[] column2table = null;
    private Int2ObjectOpenHashMap<IntSingleLinkedList> dep2ref = null;
    private Map<AttributeCombination, List<AttributeCombination>> naryDep2ref = null;

    @Override
    public String toString() {
        return PrintUtils.toString(this);
    }

    protected String getAuthorName() {
        return "Jakob Leander Müller & Thorsten Papenbrock";
    }

    protected String getDescriptionText() {
        return "Partial Divide and Conquer-based IND discovery";
    }

    public void execute() throws AlgorithmExecutionException {
        // Disable Logging (FastSet sometimes complains about skewed key distributions with lots of warnings)
        LoggingUtils.disableLogging();

        try {
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
            Buckets.bucketize(this);
            this.unaryLoadTime = System.currentTimeMillis() - this.unaryLoadTime;

            //////////////////////////////////////////////////////
            // Phase 2: Checking (Check INDs using the buckets) //
            //////////////////////////////////////////////////////
            this.unaryCompareTime = System.currentTimeMillis();
            this.checkViaTwoStageIndexAndLists();
            this.unaryCompareTime = System.currentTimeMillis() - this.unaryCompareTime;

            /////////////////////////////////////////////////////////
            // Phase 3: N-ary IND detection (Find INDs of size > 1 //
            /////////////////////////////////////////////////////////
            if (this.detectNary)
                this.detectNaryViaBucketing();

            //////////////////////////////////////////////////////
            // Phase 4: Output (Return and/or write the results //
            //////////////////////////////////////////////////////
            this.outputTime = System.currentTimeMillis();
            this.output();
            this.outputTime = System.currentTimeMillis() - this.outputTime;

            System.out.println(this);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new AlgorithmExecutionException(e.getMessage());
        } finally {

            // Clean temp
            if (this.cleanTemp)
                FileUtils.cleanDirectory(this.tempFolder);
        }
    }

    private void checkViaTwoStageIndexAndLists() throws IOException {
        System.out.println("Checking ...");

        /////////////////////////////////////////////////////////
        // Phase 2.1: Pruning (Dismiss first candidates early) //
        /////////////////////////////////////////////////////////

        // Set up the initial INDs using type information
        IntArrayList strings = new IntArrayList(this.numColumns / 2);
        IntArrayList numerics = new IntArrayList(this.numColumns / 2);
        IntArrayList temporals = new IntArrayList();
        IntArrayList unknown = new IntArrayList();
        for (int column = 0; column < this.numColumns; column++) {
            if (DatabaseUtils.isString(this.columnTypes.get(column)))
                strings.add(column);
            else if (DatabaseUtils.isNumeric(this.columnTypes.get(column)))
                numerics.add(column);
            else if (DatabaseUtils.isTemporal(this.columnTypes.get(column)))
                temporals.add(column);
            else
                unknown.add(column);
        }

        // Empty attributes can directly be placed in the output as they are contained in everything else; no empty attribute needs to be checked
        Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refFinal = new Int2ObjectOpenHashMap<>(this.numColumns);
        Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs = new Int2ObjectOpenHashMap<>(this.numColumns);
        this.fetchCandidates(strings, attribute2Refs, dep2refFinal);
        this.fetchCandidates(numerics, attribute2Refs, dep2refFinal);
        this.fetchCandidates(temporals, attribute2Refs, dep2refFinal);
        this.fetchCandidates(unknown, attribute2Refs, dep2refFinal);

        // Apply statistical pruning
        // TODO ...

        ///////////////////////////////////////////////////////////////
        // Phase 2.2: Validation (Successively check all candidates) //
        ///////////////////////////////////////////////////////////////

        // The initially active attributes are all non-empty attributes
        BitSet activeAttributes = new BitSet(this.numColumns);
        for (int column = 0; column < this.numColumns; column++)
            if (this.columnSizes.getLong(column) > 0)
                activeAttributes.set(column);

        // Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
        levelloop:
        for (int bucketNumber : this.bucketComparisonOrder) { // TODO: Externalize this code into a method and use return instead of break
            // Refine the current bucket level if it does not fit into memory at once
            int[] subBucketNumbers = Buckets.refineBucketLevel(this, activeAttributes, 0, bucketNumber);
            for (int subBucketNumber : subBucketNumbers) {
                // Identify all currently active attributes
                activeAttributes = this.getActiveAttributesFromLists(activeAttributes, attribute2Refs);
                this.activeAttributesPerBucketLevel.add(activeAttributes.cardinality());
                if (activeAttributes.isEmpty())
                    break levelloop;

                // Load next bucket level as two stage index
                Int2ObjectOpenHashMap<List<String>> attribute2Bucket = new Int2ObjectOpenHashMap<>(this.numColumns);
                Map<String, IntArrayList> invertedIndex = new HashMap<>();
                for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
                    // Build the index
                    List<String> bucket = Buckets.readBucketAsList(this, attribute, bucketNumber, subBucketNumber);
                    attribute2Bucket.put(attribute, bucket);
                    // Build the inverted index
                    for (String value : bucket) {
                        if (!invertedIndex.containsKey(value))
                            invertedIndex.put(value, new IntArrayList(2));
                        invertedIndex.get(value).add(attribute);
                    }
                }

                // Check INDs
                for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
                    for (String value : attribute2Bucket.get(attribute)) {
                        // Break if the attribute does not reference any other attribute
                        if (attribute2Refs.get(attribute).isEmpty())
                            break;

                        // Continue if the current value has already been handled
                        if (!invertedIndex.containsKey(value))
                            continue;

                        // Prune using the group of attributes containing the current value
                        IntArrayList sameValueGroup = invertedIndex.get(value);
                        this.prune(attribute2Refs, sameValueGroup);

                        // Remove the current value from the index as it has now been handled
                        invertedIndex.remove(value);
                    }
                }
            }
        }

        // Remove deps that have no refs
        IntIterator depIterator = attribute2Refs.keySet().iterator();
        while (depIterator.hasNext()) {
            if (attribute2Refs.get(depIterator.nextInt()).isEmpty())
                depIterator.remove();
        }
        this.dep2ref = attribute2Refs;
        this.dep2ref.putAll(dep2refFinal);
    }

    private void fetchCandidates(IntArrayList columns, Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refToCheck, Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refFinal) {
        IntArrayList nonEmptyColumns = new IntArrayList(columns.size());
        for (int column : columns)
            if (this.columnSizes.getLong(column) > 0)
                nonEmptyColumns.add(column);

        if (this.filterKeyForeignkeys) {
            for (int dep : columns) {
                // Empty columns are no foreign keys
                if (this.columnSizes.getLong(dep) == 0)
                    continue;

                // Referenced columns must not have null values and must come from different tables
                IntArrayList seed = nonEmptyColumns.clone();
                IntListIterator iterator = seed.iterator();
                while (iterator.hasNext()) {
                    int ref = iterator.nextInt();
                    if ((this.column2table[dep] == this.column2table[ref]) || this.nullValueColumns.get(ref))
                        iterator.remove();
                }

                dep2refToCheck.put(dep, new IntSingleLinkedList(seed, dep));
            }
        } else {
            for (int dep : columns) {
                if (this.columnSizes.getLong(dep) == 0)
                    dep2refFinal.put(dep, new IntSingleLinkedList(columns, dep));
                else
                    dep2refToCheck.put(dep, new IntSingleLinkedList(nonEmptyColumns, dep));
            }
        }
    }

    private void prune(Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs, IntArrayList attributeGroup) {
        for (int attribute : attributeGroup)
            attribute2Refs.get(attribute).retainAll(attributeGroup);
    }


    private BitSet getActiveAttributesFromLists(BitSet previouslyActiveAttributes, Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs) {
        BitSet activeAttributes = new BitSet(this.numColumns);
        for (int attribute = previouslyActiveAttributes.nextSetBit(0); attribute >= 0; attribute = previouslyActiveAttributes.nextSetBit(attribute + 1)) {
            // All attributes referenced by this attribute are active
            attribute2Refs.get(attribute).setOwnValuesIn(activeAttributes);
            // This attribute is active if it references any other attribute
            if (!attribute2Refs.get(attribute).isEmpty())
                activeAttributes.set(attribute);
        }
        return activeAttributes;
    }


    private void detectNaryViaBucketing() throws InputGenerationException, InputIterationException, IOException, AlgorithmConfigurationException {
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
            AttributeCombination depAttributeCombination = new AttributeCombination(this.column2table[dep], dep);
            List<AttributeCombination> refAttributeCombinations = new LinkedList<>();

            ElementIterator refIterator = this.dep2ref.get(dep).elementIterator();
            while (refIterator.hasNext()) {
                int ref = refIterator.next();
                refAttributeCombinations.add(new AttributeCombination(this.column2table[ref], ref));
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
            this.naryCheckViaTwoStageIndexAndLists(nPlusOneAryDep2ref, attributeCombinations, naryOffset);

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

                        // We want the lhs and rhs to be disjunct, because INDs with non-disjunct sides usually don't have practical relevance; remove this check if INDs with overlapping sides are of interest
                        if ((depPivotAttr == refExtensionAttr) || (depExtensionAttr == refPivotAttr))
                            continue;
                        //if (nPlusOneDep.contains(nPlusOneRef.getAttributes()[previousSize - 1]) ||
                        //	nPlusOneRef.contains(nPlusOneDep.getAttributes()[previousSize - 1]))
                        //	continue;

                        // The new candidate was created with two lhs and their rhs that share the same prefix; but other subsets of the lhs and rhs must also exist if the new candidate is larger than two attributes
                        // TODO: Test if the other subsets exist as well (because this test is expensive, same prefix of two INDs might be a strong enough filter for now)

                        // Merge the dep attributes and ref attributes, respectively
                        AttributeCombination nPlusOneDep = new AttributeCombination(depPivot.getTable(), depPivot.getAttributes(), depExtensionAttr);
                        AttributeCombination nPlusOneRef = new AttributeCombination(refPivot.getTable(), refPivot.getAttributes(), refExtensionAttr);

                        // Store the new candidate
                        if (!nPlusOneAryDep2ref.containsKey(nPlusOneDep))
                            nPlusOneAryDep2ref.put(nPlusOneDep, new LinkedList<AttributeCombination>());
                        nPlusOneAryDep2ref.get(nPlusOneDep).add(nPlusOneRef);

//System.out.println(CollectionUtils.concat(nPlusOneDep.getAttributes(), ",") + "c" + CollectionUtils.concat(nPlusOneRef.getAttributes(), ","));
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

    private void naryBucketize(List<AttributeCombination> attributeCombinations, int naryOffset, int[] narySpillCounts) throws InputGenerationException, InputIterationException, IOException, AlgorithmConfigurationException {
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
            Int2ObjectOpenHashMap<List<Set<String>>> buckets = new Int2ObjectOpenHashMap<>(numTableAttributeCombinations);
            for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                List<Set<String>> attributeCombinationBuckets = new ArrayList<>();
                for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++)
                    attributeCombinationBuckets.add(new HashSet<String>());
                buckets.put(attributeCombinationNumber, attributeCombinationBuckets);
            }

            // Initialize value counters
            int numValuesSinceLastMemoryCheck = 0;
            int[] numValuesInAttributeCombination = new int[attributeCombinations.size()];
            for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
                numValuesInAttributeCombination[attributeCombinationNumber] = 0;

            // Load data
            InputIterator inputIterator = null;
            try {
                inputIterator = new FileInputIterator(this.fileInputGenerator[tableIndex], this.inputRowLimit);

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
                        if (anyNull) continue;

                        String valueSeparator = "#";
                        String value = CollectionUtils.concat(attributeCombinationValues, valueSeparator);

                        // Bucketize
                        int bucketNumber = Buckets.calculateBucketFor(value, this.numBucketsPerColumn);
                        if (buckets.get(attributeCombinationNumber).get(bucketNumber).add(value)) {
                            numValuesSinceLastMemoryCheck++;
                            numValuesInAttributeCombination[attributeCombinationNumber] = numValuesInAttributeCombination[attributeCombinationNumber] + 1;
                        }

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

                                // Write buckets from largest column to disk and empty written buckets
                                for (int largeBucketNumber = 0; largeBucketNumber < this.numBucketsPerColumn; largeBucketNumber++) {
                                    Buckets.writeBucket(this.tempFolder, naryOffset + largestAttributeCombinationNumber, largeBucketNumber, -1, buckets.get(largestAttributeCombinationNumber).get(largeBucketNumber), this.columnSizes);
                                    buckets.get(largestAttributeCombinationNumber).set(largeBucketNumber, new HashSet<String>());
                                }

                                numValuesInAttributeCombination[largestAttributeCombinationNumber] = 0;

                                narySpillCounts[largestAttributeCombinationNumber] = narySpillCounts[largestAttributeCombinationNumber] + 1;

                                System.gc();
                            }
                        }
                    }
                }
            } finally {
                FileUtils.close(inputIterator);
            }

            // Write buckets to disk
            for (int attributeCombinationNumber : table2attributeCombinationNumbers.get(tableIndex)) {
                if (narySpillCounts[attributeCombinationNumber] == 0) { // if a attribute combination was spilled to disk, we do not count empty buckets for this attribute combination, because the partitioning distributes the values evenly and hence all buckets should have been populated
                    for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
                        Set<String> bucket = buckets.get(attributeCombinationNumber).get(bucketNumber);
                        if (bucket.size() != 0)
                            Buckets.writeBucket(this.tempFolder, naryOffset + attributeCombinationNumber, bucketNumber, -1, bucket, this.columnSizes);
                        else
                            emptyBuckets[bucketNumber] = emptyBuckets[bucketNumber] + 1;
                    }
                } else {
                    for (int bucketNumber = 0; bucketNumber < this.numBucketsPerColumn; bucketNumber++) {
                        Set<String> bucket = buckets.get(attributeCombinationNumber).get(bucketNumber);
                        if (bucket.size() != 0)
                            Buckets.writeBucket(this.tempFolder, naryOffset + attributeCombinationNumber, bucketNumber, -1, bucket, this.columnSizes);
                    }
                }
            }
        }

        // Calculate the bucket comparison order from the emptyBuckets to minimize the influence of sparse-attribute-issue
        Buckets.calculateBucketComparisonOrder(emptyBuckets, this.numBucketsPerColumn, this.numColumns, this);
    }

    private void naryCheckViaTwoStageIndexAndLists(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations, int naryOffset) throws IOException {
        ////////////////////////////////////////////////////
        // Validation (Successively check all candidates) //
        ////////////////////////////////////////////////////

        // Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
        BitSet activeAttributeCombinations = new BitSet(attributeCombinations.size());
        activeAttributeCombinations.set(0, attributeCombinations.size());
        levelloop:
        for (int bucketNumber : this.bucketComparisonOrder) { // TODO: Externalize this code into a method and use return instead of break
            // Refine the current bucket level if it does not fit into memory at once
            int[] subBucketNumbers = Buckets.refineBucketLevel(this, activeAttributeCombinations, naryOffset, bucketNumber);
            for (int subBucketNumber : subBucketNumbers) {
                // Identify all currently active attributes
                activeAttributeCombinations = this.getActiveAttributeCombinations(activeAttributeCombinations, naryDep2ref, attributeCombinations);
                this.naryActiveAttributesPerBucketLevel.add(activeAttributeCombinations.cardinality());
                if (activeAttributeCombinations.isEmpty())
                    break levelloop;

                // Load next bucket level as two stage index
                Int2ObjectOpenHashMap<List<String>> attributeCombination2Bucket = new Int2ObjectOpenHashMap<>();
                Map<String, IntArrayList> invertedIndex = new HashMap<>();
                for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination = activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
                    // Build the index
                    List<String> bucket = Buckets.readBucketAsList(this, naryOffset + attributeCombination, bucketNumber, subBucketNumber);
                    attributeCombination2Bucket.put(attributeCombination, bucket);
                    // Build the inverted index
                    for (String value : bucket) {
                        if (!invertedIndex.containsKey(value))
                            invertedIndex.put(value, new IntArrayList(2));
                        invertedIndex.get(value).add(attributeCombination);
                    }
                }

                // Check INDs
                for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination = activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
                    for (String value : attributeCombination2Bucket.get(attributeCombination)) {
                        // Break if the attribute combination does not reference any other attribute combination
                        if (!naryDep2ref.containsKey(attributeCombinations.get(attributeCombination)) || (naryDep2ref.get(attributeCombinations.get(attributeCombination)).isEmpty()))
                            break;

                        // Continue if the current value has already been handled
                        if (!invertedIndex.containsKey(value))
                            continue;

                        // Prune using the group of attributes containing the current value
                        IntArrayList sameValueGroup = invertedIndex.get(value);
                        this.prune(naryDep2ref, sameValueGroup, attributeCombinations);

                        // Remove the current value from the index as it has now been handled
                        invertedIndex.remove(value);
                    }
                }
            }
        }

        // Format the results
        Iterator<AttributeCombination> depIterator = naryDep2ref.keySet().iterator();
        while (depIterator.hasNext()) {
            if (naryDep2ref.get(depIterator.next()).isEmpty())
                depIterator.remove();
        }
    }

    private BitSet getActiveAttributeCombinations(BitSet previouslyActiveAttributeCombinations, Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations) {
        BitSet activeAttributeCombinations = new BitSet(attributeCombinations.size());
        for (int attribute = previouslyActiveAttributeCombinations.nextSetBit(0); attribute >= 0; attribute = previouslyActiveAttributeCombinations.nextSetBit(attribute + 1)) {
            AttributeCombination attributeCombination = attributeCombinations.get(attribute);
            if (naryDep2ref.containsKey(attributeCombination)) {
                // All attribute combinations referenced by this attribute are active
                for (AttributeCombination refAttributeCombination : naryDep2ref.get(attributeCombination))
                    activeAttributeCombinations.set(attributeCombinations.indexOf(refAttributeCombination));
                // This attribute combination is active if it references any other attribute
                if (!naryDep2ref.get(attributeCombination).isEmpty())
                    activeAttributeCombinations.set(attribute);
            }
        }
        return activeAttributeCombinations;
    }

    private void prune(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, IntArrayList attributeCombinationGroupIndexes, List<AttributeCombination> attributeCombinations) {
        List<AttributeCombination> attributeCombinationGroup = new ArrayList<>(attributeCombinationGroupIndexes.size());
        for (int attributeCombinationIndex : attributeCombinationGroupIndexes)
            attributeCombinationGroup.add(attributeCombinations.get(attributeCombinationIndex));

        for (AttributeCombination attributeCombination : attributeCombinationGroup)
            if (naryDep2ref.containsKey(attributeCombination))
                naryDep2ref.get(attributeCombination).retainAll(attributeCombinationGroup);
    }

    private void output() throws CouldNotReceiveResultException, ColumnNameMismatchException {
        System.out.println("Generating output ...");

        // Output unary INDs
        for (int dep : this.dep2ref.keySet()) {
            String depTableName = this.getTableNameFor(dep, this.tableColumnStartIndexes);
            String depColumnName = this.columnNames.get(dep);

            ElementIterator refIterator = this.dep2ref.get(dep).elementIterator();
            while (refIterator.hasNext()) {
                int ref = refIterator.next();

                String refTableName = this.getTableNameFor(ref, this.tableColumnStartIndexes);
                String refColumnName = this.columnNames.get(ref);

                this.resultReceiver.receiveResult(new InclusionDependency(new ColumnPermutation(new ColumnIdentifier(depTableName, depColumnName)), new ColumnPermutation(new ColumnIdentifier(refTableName, refColumnName))));
                this.numUnaryINDs++;
            }
        }

        // Output n-ary INDs
        if (this.naryDep2ref == null)
            return;
        for (AttributeCombination depAttributeCombination : this.naryDep2ref.keySet()) {
            ColumnPermutation dep = this.buildColumnPermutationFor(depAttributeCombination);

            for (AttributeCombination refAttributeCombination : this.naryDep2ref.get(depAttributeCombination)) {
                ColumnPermutation ref = this.buildColumnPermutationFor(refAttributeCombination);

                this.resultReceiver.receiveResult(new InclusionDependency(dep, ref));
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

    private ColumnPermutation buildColumnPermutationFor(AttributeCombination attributeCombination) {
        String tableName = this.tableNames[attributeCombination.getTable()];

        List<ColumnIdentifier> columnIdentifiers = new ArrayList<>(attributeCombination.getAttributes().length);
        for (int attributeIndex : attributeCombination.getAttributes())
            columnIdentifiers.add(new ColumnIdentifier(tableName, this.columnNames.get(attributeIndex)));

        return new ColumnPermutation(columnIdentifiers.toArray(new ColumnIdentifier[0]));
    }
}
