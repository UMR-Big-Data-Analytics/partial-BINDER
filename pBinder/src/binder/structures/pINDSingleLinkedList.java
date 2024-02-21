package binder.structures;

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

    public boolean isEmpty() {
        this.initialize();

        return this.first == null;
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
