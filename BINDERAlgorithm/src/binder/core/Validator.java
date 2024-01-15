package binder.core;

import binder.structures.AttributeCombination;
import binder.structures.IntSingleLinkedList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.io.IOException;
import java.util.*;

public class Validator {

    protected static void checkViaTwoStageIndexAndLists(BINDER binder) throws IOException {
        System.out.println("Checking ...");

        /////////////////////////////////////////////////////////
        // Phase 2.1: Pruning (Dismiss first candidates early) //
        /////////////////////////////////////////////////////////

        // Set up the initial INDs using type information
        IntArrayList strings = new IntArrayList(binder.numColumns / 2);
        IntArrayList numerics = new IntArrayList(binder.numColumns / 2);
        IntArrayList temporal = new IntArrayList();
        IntArrayList unknown = new IntArrayList();
        for (int column = 0; column < binder.numColumns; column++) {
                unknown.add(column);
        }

        // Empty attributes can directly be placed in the output as they are contained in everything else; no empty attribute needs to be checked
        Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refFinal = new Int2ObjectOpenHashMap<>(binder.numColumns);
        Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs = new Int2ObjectOpenHashMap<>(binder.numColumns);
        fetchCandidates(binder, strings, attribute2Refs, dep2refFinal);
        fetchCandidates(binder, numerics, attribute2Refs, dep2refFinal);
        fetchCandidates(binder, temporal, attribute2Refs, dep2refFinal);
        fetchCandidates(binder, unknown, attribute2Refs, dep2refFinal);

        // Apply statistical pruning

        // The initially active attributes are all non-empty attributes
        BitSet activeAttributes = new BitSet(binder.numColumns);
        for (int column = 0; column < binder.numColumns; column++)
            if (binder.columnSizes.getLong(column) > 0)
                activeAttributes.set(column);

        // Iterate the buckets for all remaining INDs until the end is reached or no more INDs exist
        levelLoop(binder, attribute2Refs, activeAttributes);

        // Remove deps that have no refs
        IntIterator depIterator = attribute2Refs.keySet().iterator();
        while (depIterator.hasNext()) {
            if (attribute2Refs.get(depIterator.nextInt()).isEmpty())
                depIterator.remove();
        }
        binder.dep2ref = attribute2Refs;
        binder.dep2ref.putAll(dep2refFinal);
    }

    private static void levelLoop(BINDER binder, Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs, BitSet activeAttributes) throws IOException {
        for (int bucketNumber : binder.bucketComparisonOrder) {
            // Refine the current bucket level if it does not fit into memory at once
            int[] subBucketNumbers = Bucketizer.refineBucketLevel(binder, activeAttributes, 0, bucketNumber);
            for (int subBucketNumber : subBucketNumbers) {
                // Identify all currently active attributes
                activeAttributes = getActiveAttributesFromLists(binder, activeAttributes, attribute2Refs);
                binder.activeAttributesPerBucketLevel.add(activeAttributes.cardinality());
                if (activeAttributes.isEmpty())
                    return;

                // Load next bucket level as two stage index
                Int2ObjectOpenHashMap<List<String>> attribute2Bucket = new Int2ObjectOpenHashMap<>(binder.numColumns);
                Map<String, IntArrayList> invertedIndex = new HashMap<>();
                for (int attribute = activeAttributes.nextSetBit(0); attribute >= 0; attribute = activeAttributes.nextSetBit(attribute + 1)) {
                    // Build the index
                    List<String> bucket = Bucketizer.readBucketAsList(binder, attribute, bucketNumber, subBucketNumber);
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
                        prune(attribute2Refs, sameValueGroup);

                        // Remove the current value from the index as it has now been handled
                        invertedIndex.remove(value);
                    }
                }
            }
        }
    }

    private static void fetchCandidates(BINDER binder, IntArrayList columns, Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refToCheck, Int2ObjectOpenHashMap<IntSingleLinkedList> dep2refFinal) {
        IntArrayList nonEmptyColumns = new IntArrayList(columns.size());
        for (int column : columns)
            if (binder.columnSizes.getLong(column) > 0)
                nonEmptyColumns.add(column);

        if (binder.filterKeyForeignKeys) {
            for (int dep : columns) {
                // Empty columns are no foreign keys
                if (binder.columnSizes.getLong(dep) == 0)
                    continue;

                // Referenced columns must not have null values and must come from different tables
                IntArrayList seed = nonEmptyColumns.clone();
                IntListIterator iterator = seed.iterator();
                while (iterator.hasNext()) {
                    int ref = iterator.nextInt();
                    if ((binder.column2table[dep] == binder.column2table[ref]) || binder.nullValueColumns.get(ref))
                        iterator.remove();
                }

                dep2refToCheck.put(dep, new IntSingleLinkedList(seed, dep));
            }
        } else {
            for (int dep : columns) {
                if (binder.columnSizes.getLong(dep) == 0)
                    dep2refFinal.put(dep, new IntSingleLinkedList(columns, dep));
                else
                    dep2refToCheck.put(dep, new IntSingleLinkedList(nonEmptyColumns, dep));
            }
        }
    }

    private static void prune(Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs, IntArrayList attributeGroup) {
        for (int attribute : attributeGroup)
            attribute2Refs.get(attribute).retainAll(attributeGroup);
    }


    private static BitSet getActiveAttributesFromLists(BINDER binder, BitSet previouslyActiveAttributes, Int2ObjectOpenHashMap<IntSingleLinkedList> attribute2Refs) {
        BitSet activeAttributes = new BitSet(binder.numColumns);
        for (int attribute = previouslyActiveAttributes.nextSetBit(0); attribute >= 0; attribute = previouslyActiveAttributes.nextSetBit(attribute + 1)) {
            // All attributes referenced by this attribute are active
            attribute2Refs.get(attribute).setOwnValuesIn(activeAttributes);
            // This attribute is active if it references any other attribute
            if (!attribute2Refs.get(attribute).isEmpty())
                activeAttributes.set(attribute);
        }
        return activeAttributes;
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
            if (naryDep2ref.get(depIterator.next()).isEmpty())
                depIterator.remove();
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
                if (activeAttributeCombinations.isEmpty())
                    return;

                // Load next bucket level as two stage index
                Int2ObjectOpenHashMap<List<String>> attributeCombination2Bucket = new Int2ObjectOpenHashMap<>();
                Map<String, IntArrayList> invertedIndex = new HashMap<>();
                for (int attributeCombination = activeAttributeCombinations.nextSetBit(0); attributeCombination >= 0; attributeCombination = activeAttributeCombinations.nextSetBit(attributeCombination + 1)) {
                    // Build the index
                    List<String> bucket = Bucketizer.readBucketAsList(binder, naryOffset + attributeCombination, bucketNumber, subBucketNumber);
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
                if (!naryDep2ref.get(attributeCombination).isEmpty())
                    activeAttributeCombinations.set(attribute);
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
}
