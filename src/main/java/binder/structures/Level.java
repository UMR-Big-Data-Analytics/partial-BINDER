package binder.structures;

public record Level(int number, int emptyBuckets) implements Comparable<Level> {

	@Override
	public int compareTo(Level other) {
		if (this.number == other.number)
			return 0;
		return Integer.compare(this.emptyBuckets, other.emptyBuckets);
	}

	@Override
	public int hashCode() {
		return this.number;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Level other))
			return false;
		return (this.number == other.number) || (this.emptyBuckets == other.emptyBuckets);
	}

	@Override
	public String toString() {
		return "Level(" + this.number + "," + this.emptyBuckets + ")";
	}
}
