package ru.mail.polis.levsaskov;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinaryHeap {
    private List<PeekIterator> list = new ArrayList<>();

    public void add(PeekIterator iterator) {
        list.add(iterator);
        int currInd = getSize() - 1;
        int parent = (currInd - 1) / 2;

        while (currInd > 0 && compare(parent, currInd) > 0) {
            Collections.swap(list, currInd, parent);
            currInd = parent;
            parent = (currInd - 1) / 2;
        }
    }

    public void buildHeap(List<PeekIterator> sourceList) {
        list = sourceList;
        for (int currInd = getSize() / 2; currInd >= 0; currInd--) {
            heapify(currInd);
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

    public void heapify(int ind) {
        int currInd = ind;
        int leftChild;
        int rightChild;
        int minChild;

        for (; ; ) {
            leftChild = 2 * currInd + 1;
            rightChild = 2 * currInd + 2;
            minChild = currInd;

            if (leftChild < getSize() && compare(leftChild, minChild) < 0) {
                minChild = leftChild;
            }

            if (rightChild < getSize() && compare(rightChild, minChild) < 0) {
                minChild = rightChild;
            }

            if (minChild == currInd) {
                break;
            }

            Collections.swap(list, currInd, minChild);
            currInd = minChild;
        }
    }

    public int getSize() {
        return list.size();
    }

    private int compare(int ind1, int ind2) {
        return list.get(ind1).peek().key().compareTo(list.get(ind2).peek().key());
    }
}
