package binder.structures;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.BitSet;
import java.util.Collection;

public class IntSingleLinkedList {

    private final int except;
    private Element first = null;
    private Element last = null;
    private Collection<Integer> seed;

    public IntSingleLinkedList(Collection<Integer> seed, int except) {
        this.seed = seed;
        this.except = except;
    }

    public Element getFirst() {
        return this.first;
    }

    public Element getLast() {
        return this.last;
    }

    private void initialize() {
        if (this.seed != null) {
            for (int value : this.seed)
                if (value != this.except)
                    this.selfAdd(value);
            this.seed = null;
        }
    }

    private void selfAdd(int value) {
        Element element = new Element(value);
        if (this.last == null)
            this.first = element;
        else
            this.last.next = element;
        this.last = element;
    }

    public void add(int value) {
        this.initialize();
        this.selfAdd(value);
    }

    public void addAll(IntSingleLinkedList values) {
        this.initialize();
        ElementIterator iterator = values.elementIterator();
        while (iterator.hasNext())
            this.selfAdd(iterator.next());
    }

    public boolean isEmpty() {
        this.initialize();

        return this.first == null;
    }

    public boolean contains(int value) {
        this.initialize();

        Element element = this.first;
        while (element != null) {
            if (element.value == value)
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

        Element element = this.first;
        while (element != null) {
            bitSet.set(element.value);
            element = element.next;
        }
    }

    public void retainAll(IntArrayList otherList) {
        this.initialize();

        Element previous = null;
        Element current = this.first;
        while (current != null) {
            if (otherList.contains(current.value)) {
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

    public ElementIterator elementIterator() {
        this.initialize();

        return new ElementIterator();
    }

    public static class Element {

        public int value;
        public Element next = null;

        public Element(int value) {
            this.value = value;
        }
    }

    public class ElementIterator {

        private Element previous = null;
        private Element current = null;
        private Element next;

        public ElementIterator() {
            this.next = first;
        }

        public boolean hasNext() {
            return this.next != null;
        }

        public int next() {
            this.previous = this.current;
            this.current = this.next;
            if (this.current != null) {
                this.next = this.current.next;
            }
            assert this.current != null;
            return this.current.value;
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
