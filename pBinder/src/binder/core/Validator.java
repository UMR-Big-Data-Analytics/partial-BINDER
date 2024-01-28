package binder.core;

import binder.structures.AttributeCombination;
import binder.structures.pINDSingleLinkedList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.IOException;
import java.util.*;

public class Validator {

    private final boolean nullIsSubset;
    private final LongArrayList columnSizes;
    private final double threshold;
    boolean nullIsNull;
    int numColumns;
    BitSet activeAttributes;

    BINDER binder;

    public Validator(BINDER binder) {
        this.nullIsNull = binder.nullIsNull;
        this.nullIsSubset = binder.nullIsSubset;

        this.numColumns = binder.numColumns;
        this.columnSizes = binder.columnSizes;

        this.threshold = binder.threshold;

        this.binder = binder;
    }

    static void naryCheckViaTwoStageIndexAndLists(BINDER binder, Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations, int naryOffset) throws IOException {
        ////////////////////////////////////////////////////
        // Validation (Successively check all candidates) //
        ////////////////////////////////////////////////////

        // Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
        BitSet activeAttributeCombinations = new BitSet(attributeCombinations.size());
        activeAttributeCombinations.set(0, attributeCombinations.size());

        levelLoop(binder, naryDep2ref, attributeCombinations, naryOffset, activeAttributeCombinations);

        // Format the results
        Iterator<AttributeCombination> depIterator = naryDep2ref.keySet().iterator();
        while (depIterator.hasNext()) {
            if (naryDep2ref.get(depIterator.next()).isEmpty()) depIterator.remove();
        }
    }

    private static void levelLoop(BINDER binder, Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations, int naryOffset, BitSet activeAttributeCombinations) throws IOException {
        for (int bucketNumber : binder.bucketComparisonOrder) {
            // Refine the current bucket level if it does not fit into memory at once
            int[] subBucketNumbers = Bucketizer.refineBucketLevel(binder, activeAttributeCombinations, naryOffset, bucketNumber);
            for (int subBucketNumber : subBucketNumbers) {
                // Identify all currently active attributes
                activeAttributeCombinations = getActiveAttributeCombinations(activeAttributeCombinations, naryDep2ref, attributeCombinations);
                binder.naryActiveAttributesPerBucketLevel.add(activeAttributeCombinations.cardinality());
                if (activeAttributeCombinations.isEmpty()) return;

                // Load next bucket level as two stage index
                Int2ObjectOpenHashMap<Map<String, Long>> attributeCombination2Bucket = new Int2ObjectOpenHashMap<>();
                Map<String, IntArrayList> invertedIndex = new HashMap<>();
                for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination = activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
                    // Build the index
                    Map<String, Long> bucket = Bucketizer.readBucketAsList(binder, naryOffset + attributeCombination, bucketNumber, subBucketNumber);
                    attributeCombination2Bucket.put(attributeCombination, bucket);
                    // Build the inverted index
                    for (String value : bucket.keySet()) {
                        if (!invertedIndex.containsKey(value)) invertedIndex.put(value, new IntArrayList(2));
                        invertedIndex.get(value).add(attributeCombination);
                    }
                }

                // Check INDs
                for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination = activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
                    for (String value : attributeCombination2Bucket.get(attributeCombination).keySet()) {
                        // Break if the attribute combination does not reference any other attribute combination
                        if (!naryDep2ref.containsKey(attributeCombinations.get(attributeCombination)) || (naryDep2ref.get(attributeCombinations.get(attributeCombination)).isEmpty()))
                            break;

                        // Continue if the current value has already been handled
                        if (!invertedIndex.containsKey(value)) continue;

                        // Prune using the group of attributes containing the current value
                        IntArrayList sameValueGroup = invertedIndex.get(value);
                        prune(naryDep2ref, sameValueGroup, attributeCombinations);

                        // Remove the current value from the index as it has now been handled
                        invertedIndex.remove(value);
                    }
                }
            }
        }
    }

    private static BitSet getActiveAttributeCombinations(BitSet previouslyActiveAttributeCombinations, Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations) {
        BitSet activeAttributeCombinations = new BitSet(attributeCombinations.size());
        for (int attribute = previouslyActiveAttributeCombinations.nextSetBit(0); attribute >= 0; attribute = previouslyActiveAttributeCombinations.nextSetBit(attribute + 1)) {
            AttributeCombination attributeCombination = attributeCombinations.get(attribute);
            if (naryDep2ref.containsKey(attributeCombination)) {
                // All attribute combinations referenced by this attribute are active
                for (AttributeCombination refAttributeCombination : naryDep2ref.get(attributeCombination))
                    activeAttributeCombinations.set(attributeCombinations.indexOf(refAttributeCombination));
                // This attribute combination is active if it references any other attribute
                if (!naryDep2ref.get(attributeCombination).isEmpty()) activeAttributeCombinations.set(attribute);
            }
        }
        return activeAttributeCombinations;
    }

    private static void prune(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, IntArrayList attributeCombinationGroupIndexes, List<AttributeCombination> attributeCombinations) {
        List<AttributeCombination> attributeCombinationGroup = new ArrayList<>(attributeCombinationGroupIndexes.size());
        for (int attributeCombinationIndex : attributeCombinationGroupIndexes)
            attributeCombinationGroup.add(attributeCombinations.get(attributeCombinationIndex));

        for (AttributeCombination attributeCombination : attributeCombinationGroup)
            if (naryDep2ref.containsKey(attributeCombination))
                naryDep2ref.get(attributeCombination).retainAll(attributeCombinationGroup);
    }

    /**
     * All attributes that share a value form an attribute group. This method prunes the existing attribute refs by
     * removing all references fom each attribute which have no more validations left.
     *
     * @param attribute2Refs Map from attribute index to referenced attributes
     * @param attributeGroup List of attribute indices that share a value
     */
    private void prune(String value, Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs, IntArrayList attributeGroup, Int2ObjectOpenHashMap<Map<String, Long>> attribute2Bucket) {
        // iterate over every attribute which is in the attribute group
        for (int dependant : attributeGroup) {
            // get occurrences of value in current attribute
            long occurrences = attribute2Bucket.get(dependant).get(value);

            // for each possible pIND
            pINDSingleLinkedList.pINDIterator referencedAttributes = attribute2Refs.get(dependant).elementIterator();
            while (referencedAttributes.hasNext()) {
                pINDSingleLinkedList.pINDElement pINDCandidate = referencedAttributes.next();
                // for every pINDCandidate we check if the value is also present
                if (!attribute2Bucket.get(pINDCandidate.referenced).containsKey(value)) {

                    // if it is not present the open violations get decreased by the number of occurrences of the value
                    pINDCandidate.violationsLeft -= occurrences;

                    // if there should not be any violations left, we remove the attribute from the pINDCandidate attributes-
                    if (pINDCandidate.violationsLeft < 0L) {
                        referencedAttributes.remove();
                    }
                }
            }
        }
    }

    /**
     * @param attribute2Refs A Map with attribute indices as keys and lists of referenced attributes by the key attribute.
     * @throws IOException if a (sub)bucket can not be found on disk.
     */
    private void levelLoop(Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs) throws IOException {
        for (int bucketNumber : binder.bucketComparisonOrder) {
            // Refine the current bucket level if it does not fit into memory at once
            int[] subBucketNumbers = Bucketizer.refineBucketLevel(binder, activeAttributes, 0, bucketNumber);

            for (int subBucketNumber : subBucketNumbers) {
                // update all currently active attributes
                updateActiveAttributesFromLists(attribute2Refs);

                // safe the number of attributes which are still active in this bucket.
                // This number will always be smaller than the previous in the list
                binder.activeAttributesPerBucketLevel.add(activeAttributes.cardinality());

                // If there are no more active attributes, all pINDs have been found.
                if (activeAttributes.isEmpty()) return;

                // the attribute2bucket Map take the attribute index as a key and returns the buckets values
                Int2ObjectOpenHashMap<Map<String, Long>> attribute2Bucket = new Int2ObjectOpenHashMap<>(numColumns);

                // the invertedIndex stores in which buckets each value exists
                Map<String, IntArrayList> invertedIndex = new HashMap<>(2 ^ 16);

                for (int attribute = getNextAttribute(); attribute != -1; attribute = getNextAttribute(++attribute)) {
                    // load the bucket of the active attribute
                    Map<String, Long> bucket = Bucketizer.readBucketAsList(binder, attribute, bucketNumber, subBucketNumber);
                    attribute2Bucket.put(attribute, bucket);

                    // Build the inverted index
                    for (String value : bucket.keySet()) {
                        if (!invertedIndex.containsKey(value)) {
                            invertedIndex.put(value, new IntArrayList());
                        }
                        invertedIndex.get(value).add(attribute);
                    }
                }

                // Check pINDs
                // iteration over each active attribute
                for (int attribute = getNextAttribute(); attribute != -1; attribute = getNextAttribute(++attribute)) {
                    // iteration over the values of the attribute
                    for (String value : attribute2Bucket.get(attribute).keySet()) {

                        // Break if the attribute does not reference any other attribute
                        if (attribute2Refs.get(attribute).isEmpty()) break;

                        // Continue if the current value has already been handled
                        if (!invertedIndex.containsKey(value)) continue;

                        // Prune using the group of attributes containing the current value
                        IntArrayList sameValueGroup = invertedIndex.get(value);
                        prune(value, attribute2Refs, sameValueGroup, attribute2Bucket);

                        // Remove the current value from the index as it has now been handled
                        invertedIndex.remove(value);
                    }
                }
            }
        }
    }

    /**
     * using the currently active attributes, this method returns the next active attribute after the given start index.
     *
     * @param fromIndex minimum index the next active attribute needs to have.
     * @return the index of the next active attribute.
     */
    private int getNextAttribute(int fromIndex) {
        return activeAttributes.nextSetBit(fromIndex);
    }

    /**
     * Helper method to get the first active Attribute
     *
     * @return the index of the first active attribute
     */
    private int getNextAttribute() {
        return getNextAttribute(0);
    }

    /**
     * Given the previously active attributes, this method updates them by check which attributes are still active.
     * An attributes becomes if it is not referenced by any other and does not reference any attributes itself.
     *
     * @param attribute2Refs The attribute references list. For each attribute this object stores the indices of all referenced attributes.
     */
    private void updateActiveAttributesFromLists(Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs) {
        BitSet activeAttributes = new BitSet(numColumns);

        // iterate over all previouslyActiveAttributes
        for (int attribute = getNextAttribute(); attribute != -1; attribute = getNextAttribute(++attribute)) {

            // All attributes referenced by this attribute are active
            attribute2Refs.get(attribute).setOwnValuesIn(activeAttributes);

            // This attribute is active if it references any other attribute
            if (!attribute2Refs.get(attribute).isEmpty()) {
                activeAttributes.set(attribute);
            }
        }
        this.activeAttributes = activeAttributes;
    }

    protected void checkViaTwoStageIndexAndLists() throws IOException {
        System.out.println("Checking ...");

        /////////////////////////////////////////////////////////
        // Phase 2.1: Pruning (Dismiss first candidates early) //
        /////////////////////////////////////////////////////////

        // TODO: Set up the initial INDs using type information
        /*
        IntArrayList strings = new IntArrayList(binder.numColumns / 2);
        IntArrayList numerics = new IntArrayList(binder.numColumns / 2);
        IntArrayList temporal = new IntArrayList();
        */
        IntArrayList unknown = new IntArrayList(binder.numColumns);
        for (int column = 0; column < numColumns; column++) {
            unknown.add(column);
        }

        // Empty attributes can directly be placed in the output as they are contained in everything else; no empty attribute needs to be checked
        Int2ObjectOpenHashMap<pINDSingleLinkedList> dep2refFinal = new Int2ObjectOpenHashMap<>(numColumns);
        Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs = new Int2ObjectOpenHashMap<>(numColumns);
        //fetchCandidates(binder, strings, attribute2Refs, dep2refFinal);
        //fetchCandidates(binder, numerics, attribute2Refs, dep2refFinal);
        //fetchCandidates(binder, temporal, attribute2Refs, dep2refFinal);
        fetchCandidates(unknown, attribute2Refs, dep2refFinal);

        // TODO: Apply statistical pruning

        // The initially active attributes are all non-empty attributes
        initializeAttributeBitSet();

        // Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
        levelLoop(attribute2Refs);

        // Remove all dependencies, where no reference exists.
        // These attributes are not a subset of anything else.
        cleanEmptyDependencies(attribute2Refs);

        binder.dep2ref = attribute2Refs;
        // add the dependency reference pairs which where already known
        binder.dep2ref.putAll(dep2refFinal);
    }

    private void initializeAttributeBitSet() {
        this.activeAttributes = new BitSet(numColumns);
        for (int column = 0; column < numColumns; column++) {
            if (columnSizes.getLong(column) > 0) {
                this.activeAttributes.set(column);
            }
        }
    }

    private void cleanEmptyDependencies(Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs) {
        IntIterator depIterator = attribute2Refs.keySet().iterator();
        while (depIterator.hasNext()) {
            // if the referenced side is empty the attribute does not form any pINDs
            if (attribute2Refs.get(depIterator.nextInt()).isEmpty()) {
                depIterator.remove();
            }
        }
    }

    private void fetchCandidates(IntArrayList columns, Int2ObjectOpenHashMap<pINDSingleLinkedList> attributes2refCheck, Int2ObjectOpenHashMap<pINDSingleLinkedList> dep2refFinal) {

        // assume all columns are empty. An empty column has no values at all
        IntArrayList nonEmptyColumns = new IntArrayList(columns.size());

        // Check for every column if they have at least one entry
        for (int column : columns) {
            if (columnSizes.getLong(column) > 0) {
                nonEmptyColumns.add(column);
            }
        }

        // Depending on null value handling there are different actions we need to take

        // if we assume null is a subset of everything (including other nulls):
        if (this.nullIsNull) {
            // iterate over all left-hand sides (dependent attributes)
            for (int dep : columns) {
                // if the left-hand side has no non-null values, it is a subset of all columns.
                // We can directly add such a column to the final set.
                if (columnSizes.getLong(dep) == 0) {
                    dep2refFinal.put(dep, new pINDSingleLinkedList(0L, columns, dep));
                } else {
                    long violations = (long) (1.0 - threshold) * columnSizes.getLong(dep);
                    attributes2refCheck.put(dep, new pINDSingleLinkedList(violations, nonEmptyColumns, dep));
                }
            }
        }

        // if we assume that null is a subset of everything but null:
        // this case is basically a foreign key search, since we do not allow nulls in the right-hand side
        else if (this.nullIsSubset) {
            // TODO: rework this case
            for (int dep : columns) {
                // Empty columns are no foreign keys
                if (binder.columnSizes.getLong(dep) == 0) continue;

                // Referenced columns must not have null values and must come from different tables
                IntArrayList seed = nonEmptyColumns.clone();
                IntListIterator iterator = seed.iterator();
                while (iterator.hasNext()) {
                    int ref = iterator.nextInt();
                    if ((binder.column2table[dep] == binder.column2table[ref]) || binder.nullValueColumns.get(ref))
                        iterator.remove();
                }

                attributes2refCheck.put(dep, new pINDSingleLinkedList(0L, seed, dep));
            }
        }

        // if we assume that null != null:
        else {
            // TODO: we need to check if the attributes have null and if under a given threshold an pIND is still possible
        }
    }
}
