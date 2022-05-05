package ru.mail.polis.egorovsyannikov;

import ru.mail.polis.BaseEntry;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class FilePeekIterator extends BasePeekIterator {

    private final Path path;
    private long endIndex;
    private long startIndex;
    private long currentFilePosition;
    private final List<Long> offsets;
    private int numberOfEntries;
    private long fileSize;
    private boolean isCompact;
    public static final int BYTESBEFOREVALUES = 13;

    public FilePeekIterator(Path path, int generation) {
        this.path = path;
        this.offsets = new ArrayList<>();
        this.generation = generation;
    }

    public Path getPath() {
        return path;
    }

    public boolean isCompact() {
        return isCompact;
    }

    public void setBoundaries(String from, String to) {
        try (DataInputStream reader = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            isCompact = reader.readBoolean();
            numberOfEntries = reader.readInt();
            endIndex = reader.readLong();
            startIndex = BYTESBEFOREVALUES;
            if (endIndex == startIndex) {
                numberOfEntries = 0;
            }

            reader.skipNBytes(endIndex - BYTESBEFOREVALUES);

            for (int i = 0; i < numberOfEntries; i++) {
                offsets.add(reader.readLong());
            }

            fileSize = reader.readLong();

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
        return current != null || currentFilePosition < endIndex;
    }

    @Override
    public BaseEntry<String> peek() {
        if (current == null) {
            current = readNext();
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

        if (offsets.isEmpty()) {
            result = BYTESBEFOREVALUES;
        } else {
            binarySearchIndexResult = fileBinarySearch(numberOfEntries - 1, key);
            currentFilePosition = offsets.get(binarySearchIndexResult);
            if (peek().key().compareTo(key) < 0) {
                result = currentFilePosition;
            } else {
                result = offsets.get(binarySearchIndexResult);
            }
        }

        current = null;
        return result;
    }

    private int fileBinarySearch(int upperBound, String targetKey) {
        int high = upperBound;
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
        try (DataInputStream reader = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            reader.skipNBytes(currentFilePosition);
            BaseEntry<String> result;
            if (reader.readBoolean()) {
                result = new BaseEntry<>(readValue(reader), readValue(reader));
            } else {
                result = new BaseEntry<>(readValue(reader), null);
            }
            currentFilePosition = fileSize - reader.available();
            return result;
        } catch (IOException e) {
            return null;
        }
    }

    public BaseEntry<String> findValueByKey(String key) {
        BaseEntry<String> result = null;
        fileBinarySearch(numberOfEntries - 1, key);
        if (peek().key().equals(key)) {
            result = next();
        }
        return result;
    }

    private String readValue(DataInputStream reader) throws IOException {
        return new String(reader.readNBytes(reader.readInt()), StandardCharsets.UTF_8);
    }
}
