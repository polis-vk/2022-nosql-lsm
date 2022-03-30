package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;
import java.util.List;

public class MergeIterator implements Iterator<BaseEntry<String>> {

    private final List<FilePeekIterator> listOfIterators;
    private BaseEntry<String> prev;

    public MergeIterator(List<FilePeekIterator> listOfIterators) {
        this.listOfIterators = listOfIterators;
        this.prev = listOfIterators.get(0).peek();
    }

    @Override
    public boolean hasNext() {
        boolean result = false;
        for(FilePeekIterator filePeekIterator: listOfIterators) {
            result |= filePeekIterator.hasNext();
        }
        return result;
    }

    @Override
    public BaseEntry<String> next() {
        for(FilePeekIterator filePeekIterator: listOfIterators) {
            while (filePeekIterator.hasNext()) {
                if (prev.key().compareTo(filePeekIterator.peek().key()) < 0) {
                    prev = filePeekIterator.next();
                    break;
                }
                filePeekIterator.next();
            }
        }
        return prev;
    }
}
