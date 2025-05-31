package binder.core;

import binder.structures.AttributeCombination;
import binder.structures.pINDSingleLinkedList;
import binder.utils.DuplicateHandling;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.io.IOException;
import java.util.*;

public class Validator {

    private final ArrayList<Long> columnSizes;
    private final double threshold;
    int numColumns;
    BitSet activeAttributes;
    List<AttributeCombination> attributeCombinations;

    PartialBinderAlgorithm binder;

    public Validator(PartialBinderAlgorithm binder) {
        this.numColumns = binder.numColumns;
        this.columnSizes = binder.columnSizes;

        this.threshold = binder.threshold;

        this.binder = binder;
    }

    private BitSet getActiveAttributeCombinations(BitSet previouslyActiveAttributeCombinations, Map<AttributeCombination, List<AttributeCombination>> naryDep2ref,
                                                  List<AttributeCombination> attributeCombinations) {
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

    void naryCheckViaTwoStageIndexAndLists(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, List<AttributeCombination> attributeCombinations, int naryOffset) throws IOException {
        ////////////////////////////////////////////////////
        // Validation (Successively check all candidates) //
        ////////////////////////////////////////////////////

        this.attributeCombinations = attributeCombinations;

        // Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
        BitSet activeAttributeCombinations = new BitSet(attributeCombinations.size());
        activeAttributeCombinations.set(0, attributeCombinations.size());

        levelLoop(naryDep2ref, naryOffset, activeAttributeCombinations);

        // Format the results by removing all dependent attribute combinations which have no referenced attribute combinations
        naryDep2ref.keySet().removeIf(attributeCombination -> naryDep2ref.get(attributeCombination).isEmpty());
    }

    private void levelLoop(Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, int naryOffset, BitSet activeAttributeCombinations) throws IOException {
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
                for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination =
                        activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
                    // Build the index
                    Map<String, Long> bucket = Bucketizer.readBucketAsList(binder, naryOffset + attributeCombination, bucketNumber, subBucketNumber);
                    attributeCombination2Bucket.put(attributeCombination, bucket);
                    // Build the inverted index
                    for (String value : bucket.keySet()) {
                        if (!invertedIndex.containsKey(value)) invertedIndex.put(value, new IntArrayList(2));
                        invertedIndex.get(value).add(attributeCombination);
                    }
                }

                // Check nary pINDs
                for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination =
                        activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
                    for (String value : attributeCombination2Bucket.get(attributeCombination).keySet()) {
                        // Break if the attribute combination does not reference any other attribute combination
                        if (!naryDep2ref.containsKey(attributeCombinations.get(attributeCombination)) || (naryDep2ref.get(attributeCombinations.get(attributeCombination)).isEmpty()))
                            break;

                        // Continue if the current value has already been handled
                        if (!invertedIndex.containsKey(value)) continue;

                        // Prune using the group of attributes containing the current value
                        IntArrayList sameValueGroup = invertedIndex.get(value);
                        prune(value, naryDep2ref, sameValueGroup, attributeCombination2Bucket);

                        // Remove the current value from the index as it has now been handled
                        invertedIndex.remove(value);
                    }
                }
            }
        }
    }

    /**
     * n-ary puring method to update the naryDep2ref object.
     * Using the attributeCombinationGroup, the method ensures that only pINDs stay valid, which are still possible.
     *
     * @param value                       String value which connects the attributes
     * @param naryDep2ref                 The current n-ary pIND candidates
     * @param attributeCombinationGroup   ids of the attributes sharing the given value
     * @param attributeCombination2Bucket maps the id of an attribute to the bucket associated with that attribute
     */
    private void prune(String value, Map<AttributeCombination, List<AttributeCombination>> naryDep2ref, IntArrayList attributeCombinationGroup, Int2ObjectOpenHashMap<Map<String,
            Long>> attributeCombination2Bucket) {
        // iterate over dependent attributes which contain the given value
        for (int dependant : attributeCombinationGroup) {
            // get number of occurrences in attribute combination
            long occurrences = attributeCombination2Bucket.get(dependant).get(value);

            // if the attribute in the attributeCombinationGroup is not only a referenced attribute, we continue with the next one
            if (!naryDep2ref.containsKey(this.attributeCombinations.get(dependant))) {
                continue;
            }

            Iterator<AttributeCombination> referenceIterator = naryDep2ref.get(this.attributeCombinations.get(dependant)).iterator();
            while (referenceIterator.hasNext()) {
                AttributeCombination reference = referenceIterator.next();

                // check if referenced combination contains the value
                if (!attributeCombination2Bucket.get(this.attributeCombinations.indexOf(reference)).containsKey(value)) {
                    reference.violationsLeft -= occurrences;

                    if (reference.violationsLeft < 0L) {
                        referenceIterator.remove();
                    }
                }
            }
        }
    }

    /**
     * All attributes that share a value form an attribute group. This method prunes the existing attribute refs by
     * removing all references fom each attribute which have no more validations left.
     *
     * @param attribute2Refs Map from attribute index to referenced attributes
     * @param attributeGroup List of attribute indices that share a value
     */
    private void prune(String value, Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs, IntArrayList attributeGroup,
                       Int2ObjectOpenHashMap<Map<String, Long>> attribute2Bucket) {
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
                    if (binder.duplicateHandling == DuplicateHandling.AWARE) {
                        pINDCandidate.violationsLeft -= occurrences;
                    } else {
                        // in an unaware setting, we only care about distinct violations
                        pINDCandidate.violationsLeft -= 1;
                    }

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
    private void discoverUnary(Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs) throws IOException {
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
                Map<String, IntArrayList> invertedIndex = new HashMap<>();

                // load the entire sub-bucket into the inverted index
                loadSubBucket(bucketNumber, subBucketNumber, attribute2Bucket, invertedIndex);

                // validate the attributes using the bucket values
                validateSubBucket(attribute2Refs, attribute2Bucket, invertedIndex);
            }
        }
    }

    private void loadSubBucket(int bucketNumber, int subBucketNumber, Int2ObjectOpenHashMap<Map<String, Long>> attribute2Bucket, Map<String, IntArrayList> invertedIndex) throws IOException {
        for (int attribute = getNextAttribute(); attribute != -1; attribute = getNextAttribute(++attribute)) {
            // load the bucket of the active attribute
            Map<String, Long> bucket = Bucketizer.readBucketAsList(binder, attribute, bucketNumber, subBucketNumber);
            attribute2Bucket.put(attribute, bucket);

            // Build the inverted index
            addBucketToIndex(invertedIndex, attribute, bucket);
        }
    }

    private void validateSubBucket(Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs, Int2ObjectOpenHashMap<Map<String, Long>> attribute2Bucket, Map<String,
            IntArrayList> invertedIndex) {
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

    private void addBucketToIndex(Map<String, IntArrayList> invertedIndex, int attribute, Map<String, Long> bucket) {
        for (String value : bucket.keySet()) {
            if (!invertedIndex.containsKey(value)) {
                invertedIndex.put(value, new IntArrayList());
            }
            invertedIndex.get(value).add(attribute);
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
        //logger.info("Starting validation");

        /////////////////////////////////////////////////////////
        // Phase 2.1: Pruning (Dismiss first candidates early) //
        /////////////////////////////////////////////////////////

        IntArrayList unknown = new IntArrayList(binder.numColumns);
        for (int column = 0; column < numColumns; column++) {
            unknown.add(column);
        }

        // Empty attributes can directly be placed in the output as they are contained in everything else; no empty attribute needs to be checked
        Int2ObjectOpenHashMap<pINDSingleLinkedList> dep2refFinal = new Int2ObjectOpenHashMap<>(numColumns);
        Int2ObjectOpenHashMap<pINDSingleLinkedList> attribute2Refs = new Int2ObjectOpenHashMap<>(numColumns);

        fetchCandidates(unknown, attribute2Refs, dep2refFinal);

        // The initially active attributes are all non-empty attributes
        initializeAttributeBitSet();

        // Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
        discoverUnary(attribute2Refs);

        // Remove all dependencies, where no reference exists.
        // These attributes are not a subset of anything else.
        cleanEmptyDependencies(attribute2Refs);

        binder.dep2ref = attribute2Refs;
        // add the dependency reference pairs which where already known
        binder.dep2ref.putAll(dep2refFinal);

        int numPINDs = 0;
        for (int i : binder.dep2ref.keySet()) {
            pINDSingleLinkedList.pINDIterator ei = binder.dep2ref.get(i).elementIterator();
            while (ei.next() != null) numPINDs++;
        }

        //logger.info("Finished validation. Found " + numPINDs + " pINDs");
    }

    private void initializeAttributeBitSet() {
        this.activeAttributes = new BitSet(numColumns);
        for (int column = 0; column < numColumns; column++) {
            if (columnSizes.get(column) > 0) {
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
            if (columnSizes.get(column) > 0) {
                nonEmptyColumns.add(column);
            }
        }

        // Depending on null value handling there are different actions we need to take
        switch (binder.nullHandling) {

            // if we assume null is a subset of everything (including other nulls):
            case SUBSET -> {
                // iterate over all left-hand sides (dependent attributes)
                for (int dep : columns) {
                    // if the left-hand side has no non-null values, it is a subset of all columns.
                    // We can directly add such a column to the final set.
                    if (columnSizes.get(dep) == 0) {
                        dep2refFinal.put(dep, new pINDSingleLinkedList(0L, columns, dep));
                    } else {
                        // TODO: account for NULLS in violations
                        long violations = (long) ((1.0 - threshold) * binder.tableSizes[binder.column2table[dep]]);
                        attributes2refCheck.put(dep, new pINDSingleLinkedList(violations, nonEmptyColumns, dep));
                    }
                }
            }

            // if we assume that null is a subset of everything but null:
            // this case is basically a foreign key search, since we do not allow nulls in the right-hand side
            case FOREIGN -> {
                // TODO: rework this case
                for (int dep : columns) {
                    // Empty columns are no foreign keys
                    if (binder.columnSizes.get(dep) == 0) continue;

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
            default -> {
                // TODO: we need to check if the attributes have null and if under a given threshold an pIND is still possible
            }
        }
    }
}
