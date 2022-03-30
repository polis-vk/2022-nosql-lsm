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
    private ArrayList<Integer> offsets;
    private int numberOfEntries;
    private Iterator<BaseEntry<String>> delegate;

    public FilePeekIterator(Path path, String from, String to) {
        this.path = path;
        this.offsets = new ArrayList<>();
        init(from, to);
    }

    public FilePeekIterator(Iterator<BaseEntry<String>> delegate) {
        this.delegate = delegate;
    }

    private void init(String from, String to) {
        try (RandomAccessFile reader = new RandomAccessFile(path.toString(), "r")) {
            numberOfEntries = reader.readInt();
            endIndex = reader.readInt();
            startIndex = 0;
            reader.seek(endIndex);
            for (int i = 0; i < numberOfEntries; i++) {
                offsets.add(reader.readInt());
            }
            if(from != null && to != null) {
                endIndex = fileBinarySearch(reader, numberOfEntries - 1, to);
                startIndex = fileBinarySearch(reader, numberOfEntries - 1, from);
            } else if(to != null) {
                endIndex = fileBinarySearch(reader, numberOfEntries - 1, to);
            } else if(from != null) {
                startIndex = fileBinarySearch(reader, numberOfEntries - 1, from);
            }

            if(startIndex < 0 || endIndex < 0) {
                currentFilePosition = -1;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        if(delegate == null) {
            return current != null || currentFilePosition < endIndex || currentFilePosition > -1;
        } else {
            return current != null || delegate.hasNext();
        }
    }

    public BaseEntry<String> peek() {
        if (current == null) {
            if(delegate == null) {
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

    private long fileBinarySearch(RandomAccessFile reader, int high, String targetKey)
            throws IOException {
        int low = 0;
        reader.seek(startIndex);
        while (low <= high) {
            int mid = low + ((high - low) / 2);
            reader.seek(offsets.get(mid));
            if (peek().key().equals(targetKey)) {
                return currentFilePosition;
            } else if (peek().key().toLowerCase(Locale.ROOT).compareTo(targetKey.toLowerCase(Locale.ROOT)) > 0) {
                high = mid - 1;
            } else if (peek().key().toLowerCase(Locale.ROOT).compareTo(targetKey.toLowerCase(Locale.ROOT)) < 0) {
                low = mid + 1;
            }
        }
        return -1;
    }

    private BaseEntry<String> readNext() {
        try (RandomAccessFile reader = new RandomAccessFile(path.toString(), "r")) {
            reader.seek(currentFilePosition);
            BaseEntry<String> result = new BaseEntry<>(reader.readUTF(), reader.readUTF());
            currentFilePosition = reader.getFilePointer();
            return result;
        } catch (IOException e) {
            return null;
        }
    }

    public BaseEntry<String> findValueByKey(String key) {
        if(delegate == null) {
            try (RandomAccessFile reader = new RandomAccessFile(path.toString(), "r")) {
                BaseEntry<String> result = null;
                if (fileBinarySearch(reader, numberOfEntries - 1, key) == 0) {
                    result = next();
                }
                reader.seek(startIndex);
                return result;
            } catch (IOException e) {
                return null;
            }
        } else {
            while (delegate.hasNext()) {
                BaseEntry<String> result = delegate.next();
                if(result.key().equals(key)) {
                    return result;
                }
            }
            return null;
        }
    }
}
