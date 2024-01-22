package binder.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;
import java.util.List;

public class CollectionUtils {


    public static int countN(int[] numbers, int numberToCount) {
        int count = 0;
        for (int number : numbers)
            if (number == numberToCount) count++;
        return count;
    }

    public static int countNotN(int[] numbers, int numberNotToCount) {
        return numbers.length - countN(numbers, numberNotToCount);
    }

    public static int countN(LongArrayList numbers, int numberToCount) {
        int count = 0;
        for (long number : numbers)
            if (number == numberToCount) count++;
        return count;
    }

    // Simply concatenate the elements of a collection
    public static <T> String concat(Iterable<T> objects, String separator) {
        if (objects == null) return "";

        StringBuilder buffer = new StringBuilder();

        for (T object : objects) {
            buffer.append((object == null) ? "null" : object.toString());
            buffer.append(separator);
        }

        if (buffer.length() > separator.length()) buffer.delete(buffer.length() - separator.length(), buffer.length());

        return buffer.toString();
    }

    // Simply concatenate the elements of an IntArrayList
    public static String concat(IntArrayList integers, String separator) {
        if (integers == null) return "";

        StringBuilder buffer = new StringBuilder();

        for (int integer : integers) {
            buffer.append(integer);
            buffer.append(separator);
        }

        if (buffer.length() > separator.length()) buffer.delete(buffer.length() - separator.length(), buffer.length());

        return buffer.toString();
    }

    // Simply concatenate the elements of an IntArrayList
    public static String concat(LongArrayList longs, String separator) {
        if (longs == null) return "";

        StringBuilder buffer = new StringBuilder();

        for (long longValue : longs) {
            buffer.append(longValue);
            buffer.append(separator);
        }

        if (buffer.length() > separator.length()) buffer.delete(buffer.length() - separator.length(), buffer.length());

        return buffer.toString();
    }

    // Simply concatenate the elements of an array
    public static String concat(Object[] objects, String separator) {
        if (objects == null) return "";

        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < objects.length; i++) {
            buffer.append(objects[i].toString());
            if ((i + 1) < objects.length) buffer.append(separator);
        }

        return buffer.toString();
    }

    // Simply concatenate the elements of an array
    public static String concat(int[] numbers, String separator) {
        if (numbers == null) return "";

        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < numbers.length; i++) {
            buffer.append(numbers[i]);
            if ((i + 1) < numbers.length) buffer.append(separator);
        }

        return buffer.toString();
    }

    // Concatenate the elements of the arrays and the whole list
    public static String concat(List<int[]> numbersList, String innerSeparator, String outerSeparator) {
        if (numbersList == null) return "";

        StringBuilder buffer = new StringBuilder();

        Iterator<int[]> iterator = numbersList.iterator();
        while (iterator.hasNext()) {
            int[] numbers = iterator.next();
            for (int i = 0; i < numbers.length; i++) {
                buffer.append(numbers[i]);
                if ((i + 1) < numbers.length) buffer.append(innerSeparator);
            }
            if (iterator.hasNext()) buffer.append(outerSeparator);
        }

        return buffer.toString();
    }

    // Concatenate the elements of an array extending each element by a given prefix and suffix
    public static String concat(String[] strings, String prefix, String suffix, String separator) {
        if (strings == null) return "";

        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < strings.length; i++) {
            buffer.append(prefix).append(strings[i]).append(suffix);
            if ((i + 1) < strings.length) buffer.append(separator);
        }

        return buffer.toString();
    }

    // Concatenate the same string multiple times
    public static String concat(int times, String string, String separator) {
        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < times; i++) {
            buffer.append(string);
            if ((i + 1) < times) buffer.append(separator);
        }

        return buffer.toString();
    }

    // Interleave and concatenate the two arrays
    public static String concat(String[] stringsA, String[] stringsB, String separatorStrings, String separatorPairs) {
        if ((stringsA == null) || (stringsB == null)) return "";

        StringBuilder buffer = new StringBuilder();

        int times = Math.max(stringsA.length, stringsB.length);
        for (int i = 0; i < times; i++) {
            if (stringsA.length > i) buffer.append(stringsA[i]);

            if ((stringsA.length > i) && (stringsB.length > i)) buffer.append(separatorStrings);

            if (stringsB.length > i) buffer.append(stringsB[i]);

            if ((i + 1) < times) buffer.append(separatorPairs);
        }

        return buffer.toString();
    }
}
