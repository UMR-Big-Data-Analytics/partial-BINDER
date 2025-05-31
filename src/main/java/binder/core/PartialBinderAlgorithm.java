package binder.core;

import binder.structures.AttributeCombination;
import binder.structures.pINDSingleLinkedList;
import binder.utils.DuplicateHandling;
import binder.utils.FileUtils;
import binder.utils.NullHandling;
import binder.utils.PrintUtils;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.RelaxedInclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.RelaxedInclusionDependency;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;


import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class PartialBinderAlgorithm {

    public RelationalInputGenerator[] fileInputGenerator = null;
    public String[] tableNames = null;
    public long[] tableSizes = null;
    public String databaseName = null;
    public boolean cleanTemp = true;
    public boolean detectNary = true;
    public int inputRowLimit = -1;
    public int numBucketsPerColumn = 10; // Initial number of buckets per column
    public int memoryCheckFrequency = 1000; // Number of new, i.e., so far unseen values during bucketing that trigger a memory consumption check
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
    protected boolean nullIsSubset = false;
    protected int maxNaryLevel = -1;
    Int2ObjectOpenHashMap<List<Map<String, Long>>> attribute2subBucketsCache = null;
    int[] tableColumnStartIndexes = null;
    List<String> columnNames = null;
    int[] column2table = null;
    Int2ObjectOpenHashMap<pINDSingleLinkedList> dep2ref = null;
    private Map<AttributeCombination, List<AttributeCombination>> naryDep2ref = null;
    RelaxedInclusionDependencyResultReceiver resultReceiver = null;

    public double threshold = 1d;
    public NullHandling nullHandling = NullHandling.SUBSET;
    public DuplicateHandling duplicateHandling = DuplicateHandling.AWARE;

    // output related settings
    public String tempFolderPath = System.getProperty("java.io.tmpdir");
    public String resultFolder = System.getProperty("java.io.tmpdir");
    public String statisticsFileName = "IND_statistics.txt";


    @Override
    public String toString() {
        return PrintUtils.toString(this);
    }

    public void execute() throws IOException, AlgorithmExecutionException {
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

        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        } finally {
            // Clean temp
            if (this.cleanTemp)
                FileUtils.cleanDirectory(this.tempFolder);
        }
    }


    private void detectNaryViaBucketing(Validator validator) throws IOException, InputIterationException {
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
            //logger.info("Starting level " + naryLevel);

            // Generate (n+1)-ary IND candidates from the already identified unary and n-ary IND candidates
            final long naryGenerationTimeCurrent = System.currentTimeMillis();

            //System.out.println("Start: " + nPlusOneAryDep2ref);
            nPlusOneAryDep2ref = this.generateNPlusOneAryCandidates(nPlusOneAryDep2ref);
            //System.out.println("End: " + nPlusOneAryDep2ref);

            if (nPlusOneAryDep2ref.isEmpty()) {
                //logger.info("There are no candidates left. All pINDs have been found.");
                break;
            }

            // Collect all attribute combinations of the current level that are possible refs or deps and enumerate them
            Set<AttributeCombination> attributeCombinationSet = new HashSet<>(nPlusOneAryDep2ref.keySet());
            for (List<AttributeCombination> columnCombination : nPlusOneAryDep2ref.values())
                attributeCombinationSet.addAll(columnCombination);
            List<AttributeCombination> attributeCombinations = new ArrayList<>(attributeCombinationSet);
            Map<AttributeCombination, List<AttributeCombination>> finalNPlusOneAryDep2ref = nPlusOneAryDep2ref;
            //logger.info("Found " + attributeCombinations.size() + " relevant attribute combinations forming " + nPlusOneAryDep2ref.keySet().stream().mapToInt(x -> finalNPlusOneAryDep2ref.get(x).size()).sum() + " candidates");

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

            BufferedWriter bw = Files.newBufferedWriter(Path.of(tempFolderPath + "temp.txt"));
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
            //logger.info("Finished Level " + naryLevel + ". Took " + String.format("%02dm %02ds %04dms", endTime / 60_000, (endTime / 1000) % 60, endTime % 1000));
        }
    }

    private Map<AttributeCombination, List<AttributeCombination>> generateNPlusOneAryCandidates(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref) {
        Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<>();

        if ((naryDep2ref == null) || (naryDep2ref.isEmpty()))
            return nPlusOneAryDep2ref;

        Map<String, Set<String>> lastLayerLookup = new HashMap<>();
        naryDep2ref.forEach((key, value) -> {
            Set<String> referenced = new HashSet<>();
            for (AttributeCombination ref : value) {
                referenced.add(ref.toString());
            }
            lastLayerLookup.put(key.toString(), referenced);
        });

        int previousSize = naryDep2ref.keySet().iterator().next().size();

        List<AttributeCombination> deps = new ArrayList<>(naryDep2ref.keySet());
        for (int i = 0; i < deps.size() - 1; i++) {
            AttributeCombination depPivot = deps.get(i);
            depExpansionLoop:
            for (int j = i + 1; j < deps.size(); j++) { // if INDs of the form AA<CD should be discovered as well, remove + 1
                AttributeCombination depExtension = deps.get(j);

                // Ensure same tables
                if (depPivot.getTable() != depExtension.getTable())
                    continue;

                // Ensure same prefix
                if (this.notSamePrefix(depPivot, depExtension))
                    continue;

                int depExtensionAttr = depExtension.getAttributes()[previousSize - 1];

                List<String> depStrings = new ArrayList<>();
                int indexOfExtension = depPivot.size();
                if (previousSize > 1) {
                    // for 3-ary or higher we need to ensure that all subsets have been validated.
                    int[] depAttributes = Arrays.copyOf(depPivot.getAttributes(), depPivot.getAttributes().length + 1);
                    depAttributes[depAttributes.length - 1] = depExtensionAttr;
                    Arrays.sort(depAttributes);
                    indexOfExtension = Arrays.binarySearch(depAttributes, depExtensionAttr);
                    for (int skipIndex = 0; skipIndex < depAttributes.length; skipIndex++) {
                        StringBuilder attributeString = new StringBuilder();
                        attributeString.append(depPivot.getTable()).append(": [");
                        for (int k = 0; k < depAttributes.length; k++) {
                            if (k == skipIndex) {
                                continue;
                            }
                            attributeString.append(depAttributes[k]).append(", ");
                        }
                        attributeString.delete(attributeString.length()-2, attributeString.length());
                        attributeString.append(']');

                        if (!lastLayerLookup.containsKey(attributeString.toString())) {
                            continue depExpansionLoop;
                        }
                        depStrings.add(attributeString.toString());
                    }
                }

                int depPivotAttr = depPivot.getAttributes()[previousSize - 1];

                // Ensure non-empty attribute extension
                if ((previousSize == 1) && ((this.columnSizes.get(depPivotAttr) == 0) || (this.columnSizes.get(depExtensionAttr) == 0)))
                    continue;

                for (AttributeCombination refPivot : naryDep2ref.get(depPivot)) {
                    refExpansionLoop:
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

                        if (previousSize > 1) {
                            // for 3-ary or higher we need to ensure that all subsets have been validated.
                            int[] refAttributes = Arrays.copyOf(refPivot.getAttributes(), refPivot.getAttributes().length + 1);
                            //refAttributes[refAttributes.length - 1] = refExtensionAttr;
                            //Arrays.sort(refAttributes);
                            /*System.arraycopy(
                                    refAttributes,                  // source array
                                    indexOfExtension,                // start copying from indexOfExtension
                                    refAttributes,                   // destination array (same array)
                                    indexOfExtension + 1,             // paste starting at indexOfExtension + 1
                                    refAttributes.length - indexOfExtension - 1 // number of elements to move
                            );*/

                            for (int k = refAttributes.length - 2; k >= indexOfExtension; k--) {
                                refAttributes[k + 1] = refAttributes[k];
                            }
                            refAttributes[indexOfExtension] = refExtensionAttr;

                            for (int skipIndex = 0; skipIndex < refAttributes.length; skipIndex++) {
                                StringBuilder attributeString = new StringBuilder();
                                attributeString.append(refPivot.getTable()).append(": [");
                                for (int k = 0; k < refAttributes.length; k++) {
                                    if (k == skipIndex) {
                                        continue;
                                    }
                                    attributeString.append(refAttributes[k]).append(", ");
                                }
                                attributeString.delete(attributeString.length()-2, attributeString.length());
                                attributeString.append(']');

                                if (!lastLayerLookup.get(depStrings.get(skipIndex)).contains(attributeString.toString())) {
                                    continue refExpansionLoop;
                                }
                            }
                        }

                        // Merge the dep attributes and ref attributes, respectively
                        AttributeCombination nPlusOneDep = new AttributeCombination(depPivot.getTable(), 0, depPivot.getAttributes(), depExtensionAttr, indexOfExtension);
                        AttributeCombination nPlusOneRef = new AttributeCombination(refPivot.getTable(), 0, refPivot.getAttributes(), refExtensionAttr, indexOfExtension);

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

    private Map<AttributeCombination, List<AttributeCombination>> generateNPlusOneAryCandidatesOld(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref) {
        Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<AttributeCombination, List<AttributeCombination>>();

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
                if (!this.samePrefix(depPivot, depExtension))
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
                        if (!this.samePrefix(refPivot, refExtension))
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
                        AttributeCombination nPlusOneDep = new AttributeCombination(depPivot.getTable(), 0, depPivot.getAttributes(), depExtensionAttr);
                        AttributeCombination nPlusOneRef = new AttributeCombination(refPivot.getTable(), 0, refPivot.getAttributes(), refExtensionAttr);

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

    /**
     * generates pIND candidates for the next layer
     *
     * @param naryDep2ref the valid pINDs of the current layer
     * @return A dependant to referenced map with (current+1) sized attributes
     */
    private Map<AttributeCombination, List<AttributeCombination>> generateNPlusOneAryCandidates2(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref) {
        Map<AttributeCombination, List<AttributeCombination>> nPlusOneAryDep2ref = new HashMap<>();

        Map<String, Set<String>> lastLayerLookup = new HashMap<>();
        naryDep2ref.forEach((key, value) -> {
            Set<String> referenced = new HashSet<>();
            for (AttributeCombination ref : value) {
                referenced.add(ref.toString());
            }
            lastLayerLookup.put(key.toString(), referenced);
        });

        if ((naryDep2ref == null) || (naryDep2ref.isEmpty()))
            return nPlusOneAryDep2ref;

        int previousSize = naryDep2ref.keySet().iterator().next().size();

        List<AttributeCombination> deps = new ArrayList<>(naryDep2ref.keySet());
        for (int i = 0; i < deps.size() - 1; i++) {
            AttributeCombination depPivot = deps.get(i);
            depExpansionLoop:
            for (int j = i + 1; j < deps.size(); j++) { // if INDs of the form AA<CD should be discovered as well, remove + 1
                AttributeCombination depExtension = deps.get(j);

                // Ensure same tables
                if (depPivot.getTable() != depExtension.getTable())
                    continue;

                // Ensure same prefix
                if (this.notSamePrefix(depPivot, depExtension))
                    continue;

                int depExtensionAttr = depExtension.getAttributes()[previousSize - 1];

                List<String> depStrings = new ArrayList<>();
                if (previousSize > 1) {
                    // for 3-ary or higher we need to ensure that all subsets have been validated.
                    int[] depAttributes = Arrays.copyOf(depPivot.getAttributes(), depPivot.getAttributes().length + 1);
                    depAttributes[depAttributes.length - 1] = depExtensionAttr;
                    for (int skipIndex = 0; skipIndex < depAttributes.length; skipIndex++) {
                        StringBuilder attributeString = new StringBuilder();
                        attributeString.append(depPivot.getTable()).append(": [");
                        for (int k = 0; k < depAttributes.length; k++) {
                            if (k == skipIndex) {
                                continue;
                            }
                            attributeString.append(depAttributes[k]).append(", ");
                        }
                        attributeString.delete(attributeString.length()-2, attributeString.length());
                        attributeString.append(']');

                        if (!lastLayerLookup.containsKey(attributeString.toString())) {
                            continue depExpansionLoop;
                        }
                        depStrings.add(attributeString.toString());
                    }
                }

                int depPivotAttr = depPivot.getAttributes()[previousSize - 1];

                // Ensure non-empty attribute extension
                if ((previousSize == 1) && ((this.columnSizes.get(depPivotAttr) == 0) || (this.columnSizes.get(depExtensionAttr) == 0)))
                    continue;

                for (AttributeCombination refPivot : naryDep2ref.get(depPivot)) {
                    refExpansionLoop:
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

                        if (previousSize > 1) {
                            // for 3-ary or higher we need to ensure that all subsets have been validated.
                            int[] refAttributes = Arrays.copyOf(refPivot.getAttributes(), refPivot.getAttributes().length + 1);
                            refAttributes[refAttributes.length - 1] = refExtensionAttr;
                            for (int skipIndex = 0; skipIndex < refAttributes.length; skipIndex++) {
                                StringBuilder attributeString = new StringBuilder();
                                attributeString.append(refPivot.getTable()).append(": [");
                                for (int k = 0; k < refAttributes.length; k++) {
                                    if (k == skipIndex) {
                                        continue;
                                    }
                                    attributeString.append(refAttributes[k]).append(", ");
                                }
                                attributeString.delete(attributeString.length()-2, attributeString.length());
                                attributeString.append(']');

                                if (!lastLayerLookup.get(depStrings.get(skipIndex)).contains(attributeString.toString())) {
                                    continue refExpansionLoop;
                                }
                            }
                        }

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
     * Given two combinations, this method checks if the first n-1 entries are unequal
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

    private boolean samePrefix(AttributeCombination combination1, AttributeCombination combination2) {
        for (int i = 0; i < combination1.size() - 1; i++)
            if (combination1.getAttributes()[i] != combination2.getAttributes()[i])
                return false;
        return true;
    }

    private void output() {

        // Output unary INDs
        for (int dep : this.dep2ref.keySet()) {
            String depTableName = this.getTableNameFor(dep, this.tableColumnStartIndexes);
            String depColumnName = this.columnNames.get(dep);

            pINDSingleLinkedList.pINDIterator refIterator = this.dep2ref.get(dep).elementIterator();
            while (refIterator.hasNext()) {
                pINDSingleLinkedList.pINDElement ref = refIterator.next();

                String refTableName = this.getTableNameFor(ref.referenced, this.tableColumnStartIndexes);
                String refColumnName = this.columnNames.get(ref.referenced);

                this.numUnaryINDs++;
                long size = tableSizes[column2table[ref.referenced]];
                long violationsleft = ref.violationsLeft;
                long allowedViolations = (long) ((1.0 - threshold) * size);
                long violations = allowedViolations - violationsleft;
                double measure = (double) (size - violations) / size;

                if (resultReceiver != null) {
                    try {
                        this.resultReceiver.receiveResult(new RelaxedInclusionDependency(new ColumnPermutation(new ColumnIdentifier(depTableName, depColumnName)), new ColumnPermutation(new ColumnIdentifier(refTableName, refColumnName)), measure));
                    } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        // Output n-ary INDs
        if (this.naryDep2ref == null)
            return;
        for (AttributeCombination depAttributeCombination : this.naryDep2ref.keySet()) {

            for (AttributeCombination refAttributeCombination : this.naryDep2ref.get(depAttributeCombination)) {


                this.numNaryINDs++;
                //System.out.println(this.tableNames[depAttributeCombination.getTable()] + ": " + getColumnNames(depAttributeCombination.getAttributes()) + " " + this.tableNames[refAttributeCombination.getTable()] + ": " + getColumnNames(refAttributeCombination.getAttributes()));
                if (resultReceiver != null) {
                    try {
                        String depTableName = this.tableNames[depAttributeCombination.getTable()];
                        String refTableName = this.tableNames[refAttributeCombination.getTable()];

                        long size = tableSizes[refAttributeCombination.getTable()];
                        long violationsleft = refAttributeCombination.violationsLeft;
                        long allowedViolations = (long) ((1.0 - threshold) * size);
                        long violations = allowedViolations - violationsleft;
                        double measure = (double) (size - violations) / size;

                        ColumnPermutation depPermutation = getColumnPermutation(depTableName, depAttributeCombination.getAttributes());
                        ColumnPermutation refPermutation = getColumnPermutation(refTableName, refAttributeCombination.getAttributes());

                        this.resultReceiver.receiveResult(new RelaxedInclusionDependency(depPermutation, refPermutation, measure));
                    } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }
    }

    private ColumnPermutation getColumnPermutation(String tableName, int[] attributes) {
        List<ColumnIdentifier> columnIdentifiers = new ArrayList<>();
        for (int attribute : attributes) {
            String columnName = columnNames.get(attribute); // columnNames must be accessible
            columnIdentifiers.add(new ColumnIdentifier(tableName, columnName));
        }
        ColumnPermutation permutation = new ColumnPermutation();
        permutation.setColumnIdentifiers(columnIdentifiers);
        return permutation;
    }

    private String getColumnNames(int[] attributes) {
        StringBuilder columnNames2 = new StringBuilder();
        for (int i = 0; i < attributes.length; i++) {
            if (i == attributes.length - 1)
                columnNames2.append(columnNames.get(attributes[i]));
            else
                columnNames2.append(columnNames.get(attributes[i])).append(", ");
        }
        return "[" + columnNames2.toString() + "]";
    }

    private String getTableNameFor(int column, int[] tableColumnStartIndexes) {
        for (int i = 1; i < tableColumnStartIndexes.length; i++)
            if (tableColumnStartIndexes[i] > column)
                return this.tableNames[i - 1];
        return this.tableNames[this.tableNames.length - 1];
    }
}
