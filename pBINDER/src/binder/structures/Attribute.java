package binder.structures;

public class Attribute implements Comparable<Attribute> {

    public final int table;
    private final int column;
    private final int attributeId;

    public long violationsLeft;
    public long distinctValues;
    public long totalValues;
    public long nulls;

    public Attribute(int table, int column, int attributeId) {
        this.table = table;
        this.column = column;
        this.attributeId = attributeId;
    }

    @Override
    public int hashCode() {
        // each attribute has a unique id
        return attributeId;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Attribute other)) return false;

        if (this.table != other.table) return false;

        return this.column == other.column;
    }

    @Override
    public String toString() {
        return this.table + ": " + column;
    }

    @Override
    public int compareTo(Attribute other) {
        if (this.table == other.table) {
            return Integer.compare(this.column, other.column);
        }
        return this.table - other.table;
    }
}
