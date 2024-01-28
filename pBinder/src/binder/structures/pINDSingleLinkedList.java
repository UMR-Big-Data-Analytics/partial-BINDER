package binder.structures;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.BitSet;
import java.util.Collection;

public class pINDSingleLinkedList {

    private final int except;
    private pINDElement first = null;
    private pINDElement last = null;
    private Collection<Integer> seed;
    private final long initialViolations;

    public pINDSingleLinkedList(long initialViolations, Collection<Integer> seed, int except) {
        this.seed = seed;
        this.except = except;
        this.initialViolations = initialViolations;
    }

    public pINDElement getFirst() {
        return this.first;
    }

    public pINDElement getLast() {
        return this.last;
    }

    private void initialize() {
        if (this.seed != null) {
            for (int value : this.seed)
                if (value != this.except)
                    this.selfAdd(value, initialViolations);
            this.seed = null;
        }
    }

    private void selfAdd(int value, long violationsLeft) {
        pINDElement element = new pINDElement(value, violationsLeft);
        if (this.last == null)
            this.first = element;
        else
            this.last.next = element;
        this.last = element;
    }

    public void add(int value, long violationsLeft) {
        this.initialize();
        this.selfAdd(value, violationsLeft);
    }

    public void addAll(pINDSingleLinkedList values) {
        this.initialize();
        pINDIterator iterator = values.elementIterator();
        while (iterator.hasNext()) {
            pINDElement next = iterator.next();
            this.selfAdd(next.referenced, next.violationsLeft);
        }

    }

    /**
     * This method should only be used to append already validated pINDs. The number of violations left is set to 0.
     * @param values The attribute indices which should be added.
     */
    public void addAll(IntSingleLinkedList values) {
        this.initialize();
        IntSingleLinkedList.ElementIterator iterator = values.elementIterator();
        while (iterator.hasNext())
            this.selfAdd(iterator.next(), 0L);
    }

    public boolean isEmpty() {
        this.initialize();

        return this.first == null;
    }

    public boolean contains(int value) {
        this.initialize();

        pINDElement element = this.first;
        while (element != null) {
            if (element.referenced == value)
                return true;
            element = element.next;
        }
        return false;
    }

    /**
     * Manipulates the given BitSet by setting all bits to one where there exists an element with value == index of bit.
     *
     * @param bitSet A BitSet into which the present values should be encoded. If the bitSet is smaller than the largest
     *               value or negative, an IndexOutOfBounce exception will be thrown
     */
    public void setOwnValuesIn(BitSet bitSet) {
        this.initialize();

        pINDElement pINDElement = this.first;
        while (pINDElement != null) {
            bitSet.set(pINDElement.referenced);
            pINDElement = pINDElement.next;
        }
    }

    public void retainAll(IntArrayList otherList) {
        this.initialize();

        pINDElement previous = null;
        pINDElement current = this.first;
        while (current != null) {
            if (otherList.contains(current.referenced)) {
                previous = current;
                current = current.next;
            } else {
                if (previous == null) {
                    this.first = current.next;
                    current.next = null;
                    if (this.first == null)
                        current = null;
                    else
                        current = this.first;

                } else {
                    previous.next = current.next;
                    current.next = null;
                    current = previous.next;
                }
            }
        }
    }

    public void clear() {
        this.first = null;
        this.last = null;
    }

    public pINDIterator elementIterator() {
        this.initialize();

        return new pINDIterator();
    }

    public static class pINDElement {

        public int referenced;
        public long violationsLeft;
        public pINDElement next = null;

        public pINDElement(int value, long violationsLeft) {
            this.referenced = value;
            this.violationsLeft = violationsLeft;
        }
    }

    public class pINDIterator {

        private pINDElement previous = null;
        private pINDElement current = null;
        private pINDElement next;

        public pINDIterator() {
            this.next = first;
        }

        public boolean hasNext() {
            return this.next != null;
        }

        public pINDElement next() {
            this.previous = this.current;
            this.current = this.next;
            if (this.current != null) {
                this.next = this.current.next;
            }
            assert this.current != null;
            return this.current;
        }

        public void remove() {
            // if there is no previous element, we simply need to point the first pointer of the List to the next entry.
            if (this.previous == null) {
                // point first to next element
                first = this.next;
                // set current to null, since it is 'deleted'
                current = null;
            }
            // if we are at the first or later entry we need to put the next pointer of the previous element to the next element.
            // This means we exclude the current element.
            else {
                // set the previous point to the next element
                this.previous.next = this.next;
                // set the current element to the previous, such that the previous will still be the previous after the next call of next()
                this.current = this.previous;
            }
        }
    }
}
