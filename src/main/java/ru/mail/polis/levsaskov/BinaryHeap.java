package ru.mail.polis.levsaskov;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinaryHeap {
    private List<PeekIterator> list = new ArrayList<>();

    public void add(PeekIterator iterator) {
        list.add(iterator);
        int i = getSize() - 1;
        int parent = (i - 1) / 2;

        while (i > 0 && compare(parent, i) > 0) {
            Collections.swap(list, i, parent);
            i = parent;
            parent = (i - 1) / 2;
        }
    }

    public void buildHeap(List<PeekIterator> sourceList) {
        list = sourceList;
        for (int i = getSize() / 2; i >= 0; i--) {
            heapify(i);
        }
    }

    public PeekIterator popMin() {
        Collections.swap(list, 0, getSize() - 1);
        PeekIterator res = list.remove(getSize() - 1);
        heapify(0);
        return res;
    }

    public PeekIterator getMin() {
        return list.get(0);
    }

    public void heapify(int i) {
        int leftChild;
        int rightChild;
        int minChild;

        for (; ; ) {
            leftChild = 2 * i + 1;
            rightChild = 2 * i + 2;
            minChild = i;

            if (leftChild < getSize() && compare(leftChild, minChild) < 0) {
                minChild = leftChild;
            }

            if (rightChild < getSize() && compare(rightChild, minChild) < 0) {
                minChild = rightChild;
            }

            if (minChild == i) {
                break;
            }

            Collections.swap(list, i, minChild);
            i = minChild;
        }
    }

    public int getSize() {
        return list.size();
    }

    private int compare(int ind1, int ind2) {
        return list.get(ind1).peek().key().compareTo(list.get(ind2).peek().key());
    }
}
