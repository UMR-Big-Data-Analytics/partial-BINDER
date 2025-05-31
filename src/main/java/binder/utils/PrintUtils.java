package binder.utils;

import binder.core.PartialBinderAlgorithm;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.BitSet;

public class PrintUtils {

    public static String toString(BitSet o) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < o.length(); i++)
            builder.append((o.get(i)) ? 1 : 0);
        builder.append("]");
        return builder.toString();
    }

    public static String toString(PartialBinderAlgorithm binder) {
        String input = binder.fileInputGenerator[0].getClass().getName() + " (" + binder.fileInputGenerator.length + ")";

        return "BINDER: \r\n\t" +
                "input: " + input + "\r\n\t" +
                "databaseName: " + binder.databaseName + "\r\n\t" +
                "inputRowLimit: " + binder.inputRowLimit + "\r\n\t" +
                "threshold: " + binder.threshold + "\r\n\t" +
                "NullHandling: " + binder.nullHandling + "\r\n\t" +
                "DuplicateHandling: " + binder.duplicateHandling + "\r\n\t" +
                "tempFolderPath: " + binder.tempFolder.getPath() + "\r\n\t" +
                "tempFolderPath: " + binder.tempFolder.getPath() + "\r\n\t" +
                "tableNames: " + ((binder.tableNames != null) ? CollectionUtils.concat(binder.tableNames, ", ") : "-") + "\r\n\t" +
                "tableSizes: " + ((binder.tableSizes != null) ? CollectionUtils.concat(LongArrayList.wrap(binder.tableSizes), ", ") : "-") + "\r\n\t" +
                "numColumns: " + binder.numColumns + " (" + ((binder.spillCounts != null) ? String.valueOf(CollectionUtils.countNotN(binder.spillCounts, 0)) : "-") + " spilled)\r\n\t" +
                "numBucketsPerColumn: " + binder.numBucketsPerColumn + "\r\n\t" +
                "bucketComparisonOrder: " + ((binder.bucketComparisonOrder != null) ? CollectionUtils.concat(binder.bucketComparisonOrder, ", ") : "-") + "\r\n\t" +
                "memoryCheckFrequency: " + binder.memoryCheckFrequency + "\r\n\t" +
                "maxMemoryUsagePercentage: " + binder.maxMemoryUsagePercentage + "%\r\n\t" +
                "availableMemory: " + binder.availableMemory + " byte (spilled when exceeding " + binder.maxMemoryUsage + " byte)\r\n\t" +
                "numBucketsPerColumn: " + binder.numBucketsPerColumn + "\r\n\t" +
                "memoryCheckFrequency: " + binder.memoryCheckFrequency + "\r\n\t" +
                "cleanTemp: " + binder.cleanTemp + "\r\n\t" +
                "detectNary: " + binder.detectNary + "\r\n\t" +
                "numUnaryINDs: " + binder.numUnaryINDs + "\r\n\t" +
                "numNaryINDs: " + binder.numNaryINDs + "\r\n\t" +
                "\r\n" +
                "nullValueColumns: " + toString(binder.nullValueColumns) +
                "\r\n" +
                "columnSizes: " + ((binder.columnSizes != null) ? binder.columnSizes : "-") + "\r\n" +
                "\r\n" +
                "activeAttributesPerBucketLevel: " + ((binder.activeAttributesPerBucketLevel != null) ? CollectionUtils.concat(binder.activeAttributesPerBucketLevel, ", ") : "-") + "\r\n" +
                "naryActiveAttributesPerBucketLevel: " + ((binder.naryActiveAttributesPerBucketLevel == null) ? "-" : CollectionUtils.concat(binder.naryActiveAttributesPerBucketLevel, ", ")) + "\r\n" +
                "\r\n" +
                "spillCounts: " + ((binder.spillCounts != null) ? CollectionUtils.concat(binder.spillCounts, ", ") : "-") + "\r\n" +
                "narySpillCounts: " + ((binder.narySpillCounts == null) ? "-" : CollectionUtils.concat(binder.narySpillCounts, ", ", "\r\n")) + "\r\n" +
                "\r\n" +
                "refinements: " + ((binder.refinements != null) ? CollectionUtils.concat(binder.refinements, ", ") : "-") + "\r\n" +
                "naryRefinements: " + ((binder.naryRefinements == null) ? "-" : CollectionUtils.concat(binder.naryRefinements, ", ", "\r\n")) + "\r\n" +
                "\r\n" +
                "unaryStatisticTime: " + binder.unaryStatisticTime + "\r\n" +
                "unaryLoadTime: " + binder.unaryLoadTime + "\r\n" +
                "unaryCompareTime: " + binder.unaryCompareTime + "\r\n" +
                "naryGenerationTime: " + binder.naryGenerationTime + "\r\n" +
                "naryLoadTime: " + binder.naryLoadTime + "\r\n" +
                "naryCompareTime: " + binder.naryCompareTime + "\r\n" +
                "outputTime: " + binder.outputTime;
    }
}
