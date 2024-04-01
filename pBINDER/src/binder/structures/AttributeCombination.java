package binder.structures;

import java.util.Arrays;

public class AttributeCombination implements Comparable<AttributeCombination> {

    private final int table;
    public long violationsLeft;
    public long nulls;
    private int[] attributes;

    public AttributeCombination(int table, long violationsLeft, int... attributes) {
        this.table = table;
        this.violationsLeft = violationsLeft;
        this.setAttributes(attributes);
    }

    public AttributeCombination(int table, long violationsLeft, int[] attributes, int attribute) {
        this.table = table;
        this.violationsLeft = violationsLeft;
        this.setAttributes(attributes, attribute);
    }

    public int size() {
        return this.attributes.length;
    }

    public int getTable() {
        return this.table;
    }

    public int[] getAttributes() {
        return this.attributes;
    }

    public void setAttributes(int[] attributes) {
        this.attributes = attributes;
    }

    public void setAttributes(int[] attributes, int attribute) {
        this.attributes = Arrays.copyOf(attributes, attributes.length + 1);
        this.attributes[attributes.length] = attribute;
    }

    @Override
    public int hashCode() {
        int code = 0;
        int multiplier = 1;
        for (int attribute : this.attributes) {
            code = code + attribute * multiplier;
            multiplier = multiplier * 10;
        }
        return code + this.table * multiplier;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AttributeCombination other)) return false;

        if (this.table != other.getTable()) return false;

        int[] otherColumns = other.getAttributes();
        if (this.attributes.length != otherColumns.length) return false;

        for (int i = 0; i < this.attributes.length; i++)
            if (this.attributes[i] != otherColumns[i]) return false;

        return true;
    }

    @Override
    public String toString() {
        return this.table + ": " + Arrays.toString(attributes);
    }

    @Override
    public int compareTo(AttributeCombination other) {
        if (this.table == other.getTable()) {
            if (this.attributes.length == other.getAttributes().length) {
                for (int i = 0; i < this.attributes.length; i++) {
                    if (this.attributes[i] < other.getAttributes()[i]) return -1;
                    if (this.attributes[i] > other.getAttributes()[i]) return 1;
                }
                return 0;
            }
            return this.attributes.length - other.getAttributes().length;
        }
        return this.table - other.getTable();
    }
}
