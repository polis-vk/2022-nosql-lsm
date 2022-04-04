package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<String>> {

    private final DaoReader reader;
    private BaseEntry<String> next;

    public FileIterator(String from, String to, DaoReader reader) throws IOException, UncheckedIOException {
        this.reader = reader;
        this.reader.setEndReadFactor(to);
        this.reader.setStartReadIndex(from, to);
        this.next = getNextEntry();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> result = next;
        next = getNextEntry();
        return result;
    }

    private BaseEntry<String> getNextEntry() {
        BaseEntry<String> result;
        try {
            result = reader.readNextEntry();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }
}
