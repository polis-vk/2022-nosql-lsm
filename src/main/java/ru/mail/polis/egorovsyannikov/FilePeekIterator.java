package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;

public class FilePeekIterator implements Iterator<BaseEntry<String>> {

    private BaseEntry<String> current;
    private Path path;
    private long endIndex;
    private long startIndex;
    private long currentFilePosition;
    private ArrayList<Long> offsets;
    private int numberOfEntries;
    private Iterator<BaseEntry<String>> delegate;
    private final int generation;

    public FilePeekIterator(Path path, String from, String to, int generation) {
        this.path = path;
        this.offsets = new ArrayList<>();
        this.generation = generation;
        init(from, to);
    }

    public int getGeneration() {
        return generation;
    }

    public FilePeekIterator(Iterator<BaseEntry<String>> delegate, int generation) {
        this.delegate = delegate;
        this.generation = generation;
    }

    private void init(String from, String to) {
        try (RandomAccessFile reader = new RandomAccessFile(path.toString(), "r")) {
            numberOfEntries = reader.readInt();
            endIndex = reader.readLong();
            startIndex = reader.getFilePointer();
            if (endIndex == startIndex) {
                numberOfEntries = 0;
            }
            reader.seek(endIndex);

            for (int i = 0; i < numberOfEntries; i++) {
                offsets.add(reader.readLong());
            }

            if (from != null) {
                startIndex = findBorderIndexes(from);
            }
            if (to != null) {
                endIndex = findBorderIndexes(to);
            }

            currentFilePosition = startIndex;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        if (delegate == null) {
            return current != null || currentFilePosition < endIndex;
        } else {
            return current != null || delegate.hasNext();
        }
    }

    public BaseEntry<String> peek() {
        if (current == null) {
            if (delegate == null) {
                current = readNext();
            } else {
                current = delegate.next();
            }
        }
        return current;
    }

    @Override
    public BaseEntry<String> next() {
        BaseEntry<String> peek = peek();
        current = null;
        return peek;
    }

    private long findBorderIndexes(String key) {
        int binarySearchIndexResult;
        long result;

        if(!offsets.isEmpty()) {
            binarySearchIndexResult = fileBinarySearch(numberOfEntries - 1, key);
            currentFilePosition = offsets.get(binarySearchIndexResult);
            if (peek().key().compareTo(key) < 0) {
                result = currentFilePosition;
            } else {
                result = offsets.get(binarySearchIndexResult);
            }
        } else {
            result = 12;
        }

        current = null;
        return result;
    }

    private int fileBinarySearch(int high, String targetKey) {
        int low = 0;
        int mid = 0;
        currentFilePosition = startIndex;
        while (low <= high) {
            mid = low + ((high - low) / 2);
            currentFilePosition = offsets.get(mid);
            if (peek().key().equals(targetKey)) {
                return mid;
            } else if (peek().key().toLowerCase(Locale.ROOT).compareTo(targetKey.toLowerCase(Locale.ROOT)) > 0) {
                high = mid - 1;
            } else if (peek().key().toLowerCase(Locale.ROOT).compareTo(targetKey.toLowerCase(Locale.ROOT)) < 0) {
                low = mid + 1;
            }
            current = null;
        }
        return mid;
    }

    private BaseEntry<String> readNext() {
        try (RandomAccessFile reader = new RandomAccessFile(path.toString(), "r")) {
            reader.seek(currentFilePosition);
            BaseEntry<String> result;
            if (reader.readBoolean()) {
                result = new BaseEntry<>(reader.readUTF(), reader.readUTF());
            } else {
                result = new BaseEntry<>(reader.readUTF(), null);
            }
            currentFilePosition = reader.getFilePointer();
            return result;
        } catch (IOException e) {
            return null;
        }
    }

    public BaseEntry<String> findValueByKey(String key) {
        if (delegate == null) {
            BaseEntry<String> result = null;
            fileBinarySearch(numberOfEntries - 1, key);
            if (peek().key().equals(key)) {
                result = next();
            }
            return result;
        } else {
            while (delegate.hasNext()) {
                BaseEntry<String> result = delegate.next();
                if (result.key().equals(key)) {
                    return result;
                }
            }
            return null;
        }
    }
}
