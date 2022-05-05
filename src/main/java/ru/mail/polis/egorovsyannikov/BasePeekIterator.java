package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;

import java.util.Iterator;

public abstract class BasePeekIterator implements Iterator<BaseEntry<String>> {

    protected BaseEntry<String> current;
    protected int generation;

    public int getGeneration() {
        return generation;
    }

    public BaseEntry<String> peek() {
        return current;
    }
}
