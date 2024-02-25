package binder.core;

import binder.io.DefaultFileInputGenerator;
import binder.runner.Config;
import binder.structures.AttributeCombination;
import binder.structures.pINDSingleLinkedList;
import binder.utils.FileUtils;
import binder.utils.PrintUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class BINDER {

    private final Logger logger = LoggerFactory.getLogger(BINDER.class);
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
    public ArrayList<Long> columnSizes = null;
    protected String tempFolderPath = "BINDER_temp"; // TODO: Use Metanome temp file functionality here (interface TempFileAlgorithm)
    protected boolean nullIsSubset = false;
    protected int maxNaryLevel = 3;
    protected Config config;
    Int2ObjectOpenHashMap<List<Map<String, Long>>> attribute2subBucketsCache = null;
    int[] tableColumnStartIndexes = null;
    List<String> columnNames = null;
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
            Bucketizer.unaryBucketize(this);
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

        // Generate, unaryBucketize and test the n-ary INDs level-wise
        this.naryDep2ref = new HashMap<>();
        this.naryGenerationTime = new LongArrayList();
        this.naryLoadTime = new LongArrayList();
        this.naryCompareTime = new LongArrayList();
        while (++naryLevel <= this.maxNaryLevel || this.maxNaryLevel <= 0) {
            logger.info("Starting level " + naryLevel);

            // Generate (n+1)-ary IND candidates from the already identified unary and n-ary IND candidates
            final long naryGenerationTimeCurrent = System.currentTimeMillis();

            nPlusOneAryDep2ref = this.generateNPlusOneAryCandidates(nPlusOneAryDep2ref);
            if (nPlusOneAryDep2ref.isEmpty()) {
                logger.info("There are no candidates left. All pINDs have been found.");
                break;
            }

            // Collect all attribute combinations of the current level that are possible refs or deps and enumerate them
            Set<AttributeCombination> attributeCombinationSet = new HashSet<>(nPlusOneAryDep2ref.keySet());
            for (List<AttributeCombination> columnCombination : nPlusOneAryDep2ref.values())
                attributeCombinationSet.addAll(columnCombination);
            List<AttributeCombination> attributeCombinations = new ArrayList<>(attributeCombinationSet);
            Map<AttributeCombination, List<AttributeCombination>> finalNPlusOneAryDep2ref = nPlusOneAryDep2ref;
            logger.info("Found " + attributeCombinations.size() + " relevant attribute combinations forming " + nPlusOneAryDep2ref.keySet().stream().mapToInt(x -> finalNPlusOneAryDep2ref.get(x).size()).sum() + " candidates");

            // Extend the columnSize array
            for (int i = 0; i < attributeCombinations.size(); i++)
                this.columnSizes.add(0L);

            int[] currentNarySpillCounts = new int[attributeCombinations.size()];
            for (int attributeCombinationNumber = 0; attributeCombinationNumber < attributeCombinations.size(); attributeCombinationNumber++)
                currentNarySpillCounts[attributeCombinationNumber] = 0;
            this.narySpillCounts.add(currentNarySpillCounts);

            int[] currentNaryRefinements = new int[this.numBucketsPerColumn];

            this.naryRefinements.add(currentNaryRefinements);

            this.naryGenerationTime.add(System.currentTimeMillis() - naryGenerationTimeCurrent);

            // Read the input dataset again and unaryBucketize all attribute combinations that are refs or deps
            long naryLoadTimeCurrent = System.currentTimeMillis();
            Bucketizer.naryBucketize(this, attributeCombinations, naryOffset, currentNarySpillCounts);
            this.naryLoadTime.add(System.currentTimeMillis() - naryLoadTimeCurrent);

            // Check the n-ary IND candidates
            long naryCompareTimeCurrent = System.currentTimeMillis();
            validator.naryCheckViaTwoStageIndexAndLists(nPlusOneAryDep2ref, attributeCombinations, naryOffset);

            this.naryDep2ref.putAll(nPlusOneAryDep2ref);

            BufferedWriter bw = Files.newBufferedWriter(Path.of("results/temp.txt"));
            for (AttributeCombination a : nPlusOneAryDep2ref.keySet()) {
                String relName = tableNames[a.getTable()];
                relName = relName.substring(0, relName.length() - 4);
                StringBuilder out = new StringBuilder();
                out.append('(');
                for (int attrId : a.getAttributes()) {
                    out.append(relName).append('.').append(columnNames.get(attrId)).append(",");
                }
                out.delete(out.length() - 1, out.length()).append(") <= (");
                for (AttributeCombination ref : nPlusOneAryDep2ref.get(a)) {
                    String refRel = tableNames[ref.getTable()];
                    refRel = refRel.substring(0, refRel.length() - 4);
                    for (int attrId : ref.getAttributes()) {
                        out.append(refRel).append('.').append(columnNames.get(attrId)).append(",");
                    }
                    out.delete(out.length() - 1, out.length()).append(") (");
                }
                out.delete(out.length() - 2, out.length());
                bw.write(out.toString());
                bw.newLine();
            }

            bw.flush();
            bw.close();
            // Add the number of created buckets for n-ary INDs of this level to the naryOffset
            naryOffset = naryOffset + attributeCombinations.size();

            this.naryCompareTime.add(System.currentTimeMillis() - naryCompareTimeCurrent);

            long endTime = System.currentTimeMillis() - naryGenerationTimeCurrent;
            logger.info("Finished Level " + naryLevel + ". Took " + String.format("%02dm %02ds %04dms", endTime / 60_000, (endTime / 1000) % 60, endTime % 1000));
        }
    }

    /**
     * generates pIND candidates for the next layer
     *
     * @param naryDep2ref the valid pINDs of the current layer
     * @return A dependant to referenced map with (current+1) sized attributes
     */
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
                if ((previousSize == 1) && ((this.columnSizes.get(depPivotAttr) == 0) || (this.columnSizes.get(depExtensionAttr) == 0)))
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
                        AttributeCombination nPlusOneDep = new AttributeCombination(depPivot.getTable(), 0, depPivot.getAttributes(), depExtensionAttr);
                        AttributeCombination nPlusOneRef = new AttributeCombination(refPivot.getTable(), 0, refPivot.getAttributes(), refExtensionAttr);

                        // Store the new candidate
                        if (!nPlusOneAryDep2ref.containsKey(nPlusOneDep))
                            nPlusOneAryDep2ref.put(nPlusOneDep, new LinkedList<>());
                        nPlusOneAryDep2ref.get(nPlusOneDep).add(nPlusOneRef);
                    }
                }
            }
        }
        return nPlusOneAryDep2ref;
    }

    /**
     * Given two combinations, this method checks if the first n-1 entries are equal
     *
     * @param combination1 the first combination
     * @param combination2 the second combination
     * @return whether any of the first n-1 attributes do not match.
     */
    private boolean notSamePrefix(AttributeCombination combination1, AttributeCombination combination2) {
        for (int i = 0; i < combination1.size() - 1; i++) {
            if (combination1.getAttributes()[i] != combination2.getAttributes()[i]) {
                return true;
            }
        }
        return false;
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
