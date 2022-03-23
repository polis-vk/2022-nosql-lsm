package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;

public class FileIterator implements Iterator<BaseEntry<String>> {

    private final DaoReader reader;
    private BaseEntry<String> next;

    public FileIterator(String from, String to, Path pathToDataFile, Path pathToOffsetsFile) throws IOException {
        this.reader = new DaoReader(pathToDataFile, pathToOffsetsFile, from, to);
        next();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> result = next;
        try {
            next = reader.readNextEntry();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }
}
