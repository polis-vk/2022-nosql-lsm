package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Stream;

public class Reader {

    private static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    private static final Entry<ByteBuffer> EMPTY_ENTRY = new BaseEntry<>(EMPTY_BYTEBUFFER, EMPTY_BYTEBUFFER);

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data;
    private final NavigableMap<Integer, Entry<Path>> pathsToPairedFiles;

    public Reader(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data,
                  NavigableMap<Integer, Entry<Path>> pathsToPairedFiles) {
        this.data = data;
        this.pathsToPairedFiles = pathsToPairedFiles.descendingMap();
    }

    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        return new MergeIterator(from, to);
    }

    public Entry<ByteBuffer> get(ByteBuffer key) throws IOException {
        Entry<ByteBuffer> result = findKeyInStorage(key);
        return result == null ? findKeyInFile(key) : result;
    }

    private Entry<ByteBuffer> findKeyInStorage(ByteBuffer key) {
        return data.get(key);
    }

    private Entry<ByteBuffer> findKeyInFile(ByteBuffer key) throws IOException {
        for (Map.Entry<Integer, Entry<Path>> pathToPairedFiles: pathsToPairedFiles.entrySet()) {
            Path pathToDataFile = pathToPairedFiles.getValue().key();
            Path pathToIndexesFile = pathToPairedFiles.getValue().value();
            Entry<ByteBuffer> result = binarySearchInFile(key, pathToDataFile, pathToIndexesFile);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private Entry<ByteBuffer> binarySearchInFile(ByteBuffer key,
                                                 Path dataFilePath, Path indexesFilePath) throws IOException {
        try (FileIterator fileIterator = new FileIterator(dataFilePath, indexesFilePath)) {
            long a = 0;
            long b = fileIterator.getIndexesFileLength() / Utils.OFFSET_VALUES_DISTANCE;
            while (b - a >= 1) {
                long c = (b + a) / 2;
                fileIterator.setIndexesFileOffset(c * Utils.OFFSET_VALUES_DISTANCE);
                Entry<ByteBuffer> curEntry = fileIterator.next();

                int compare = curEntry.key().compareTo(key);
                if (compare < 0) {
                    a = c;
                } else if (compare == 0) {
                    return curEntry;
                } else {
                    b = c;
                }
            }

            return null;
        }
    }

    private ByteBuffer readByteBuffer(RandomAccessFile dataFile) throws IOException {
        int bbSize = dataFile.readInt();
        ByteBuffer bb = ByteBuffer.allocate(bbSize);
        dataFile.getChannel().read(bb);
        bb.rewind();
        return bb;
    }

    private class MergeIterator implements Iterator<Entry<ByteBuffer>>, Closeable {

        private static final EntryComparator entryComparator = new EntryComparator();

        private final ByteBuffer from;
        private final ByteBuffer to;
        private final Map<ByteBuffer, Entry<ByteBuffer>> mergedData = new TreeMap<>();
        private Iterator<Map.Entry<ByteBuffer, Entry<ByteBuffer>>> mergedDataIterator = mergedData.entrySet().iterator();
        private final List<CloseableIterator> iterators = new ArrayList<>();
        private final List<Entry<ByteBuffer>> lastEntries = new ArrayList<>();

        public MergeIterator(ByteBuffer from, ByteBuffer to) throws IOException {
            this.from = from;
            this.to = to;

            iterators.add(new CloseableIterator(data.values().iterator()));
            boolean isFromFound = false;
            for (Map.Entry<Integer, Entry<Path>> pathToPairedFiles: pathsToPairedFiles.entrySet()) {
                Path pathToDataFile = pathToPairedFiles.getValue().key();
                Path pathToIndexesFile = pathToPairedFiles.getValue().value();
                if (from != null && !isFromFound) {
                    FileIterator fileIterator = new FileIterator(pathToDataFile, pathToIndexesFile);
                    isFromFound = setFilePointerToFromIfExists(from, pathToDataFile, pathToIndexesFile, fileIterator);
                    iterators.add(fileIterator);
                } else {
                    iterators.add(new FileIterator(pathToDataFile, pathToIndexesFile));
                }
            }
        }

        private boolean setFilePointerToFromIfExists(ByteBuffer from, Path pathToDataFile,
                    Path pathToIndexesFile, FileIterator fileIterator) throws IOException {
            if (binarySearchInFile(from, pathToDataFile, pathToIndexesFile) != null) {
                Entry<ByteBuffer> entry = fileIterator.next();
                while (entry.key() != from) {
                    entry = fileIterator.next();
                }
                return true;
            }
            return false;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = mergedDataIterator.hasNext();
            for (Iterator<Entry<ByteBuffer>> iterator: iterators) {
                hasNext |= iterator.hasNext();
            }
            if (!hasNext) {
                close();
            }
            return hasNext;
        }

        @Override
        public Entry<ByteBuffer> next() {
            if (!mergedDataIterator.hasNext()) {
                merge();
                mergedDataIterator = mergedData.entrySet().iterator();
            }
            return mergedDataIterator.next().getValue();
        }

        @Override
        public void close() {
            try {
                for (CloseableIterator closeableIterator: iterators) {
                    closeableIterator.close();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void merge() {
            mergedData.clear();
            for (int i = lastEntries.size(); i < pathsToPairedFiles.size(); ++i) {
                lastEntries.add(EMPTY_ENTRY);
            }

            fillByEntriesIfNeed();

            while (!isThresholdReached() && hasNext()) {
                fillByEntriesIfNeed();

                final Stream<Entry<ByteBuffer>> clearStream = lastEntries.stream()
                        .filter(a -> !a.key().equals(EMPTY_BYTEBUFFER));
                final Entry<ByteBuffer> firstMin = clearStream
                        .min(entryComparator::compare)
                        .get();
                final Entry<ByteBuffer> secondMin = clearStream
                        .filter(a -> !a.equals(firstMin))
                        .min(entryComparator::compare)
                        .orElse(EMPTY_ENTRY);

                mergedData.put(firstMin.key(), firstMin);
                int minIndex = lastEntries.indexOf(firstMin);
                Entry<ByteBuffer> curEntry;
                while (entryComparator.compare(firstMin, secondMin) > 0
                        && !isThresholdReached() && iterators.get(minIndex).hasNext()) {
                    curEntry = iterators.get(minIndex).next();
                    mergedData.put(curEntry.key(), curEntry);
                }
                lastEntries.set(minIndex, EMPTY_ENTRY);
            }

        }

        private void fillByEntriesIfNeed() {
            boolean isNeedToFeelByEntries = true;
            for (Entry<ByteBuffer> entry: lastEntries) {
                isNeedToFeelByEntries &= entry.equals(EMPTY_ENTRY);
            }
            if (isNeedToFeelByEntries) {
                for (int i = 0; i < lastEntries.size(); ++i) {
                    if (iterators.get(i).hasNext()) {
                        lastEntries.set(i, iterators.get(i).next());
                    }
                }
            }
        }

        private boolean isThresholdReached() {
            return mergedData.size() >= Utils.DATA_PORTION;
        }

    }

    private static class EntryComparator implements Comparator<Entry<ByteBuffer>> {
        @Override
        public int compare(Entry<ByteBuffer> o1, Entry<ByteBuffer> o2) {
            if (o1.equals(EMPTY_ENTRY)) {
                return o2.equals(EMPTY_ENTRY) ? 0 : -1;
            }
            if (o2.equals(EMPTY_ENTRY)) {
                return 1;
            }
            return o1.key().compareTo(o2.key());
        }
    }

    private class FileIterator extends CloseableIterator {

        private final RandomAccessFile dataFile;
        private final RandomAccessFile indexesFile;

        public FileIterator(Path dataFilePath, Path indexesFilePath) throws IOException {
            super(null);
            dataFile = new RandomAccessFile(dataFilePath.toString(), "r");
            indexesFile = new RandomAccessFile(indexesFilePath.toString(), "r");
        }

        @Override
        public boolean hasNext() {
            try {
                return dataFile.getFilePointer() < dataFile.length();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Entry<ByteBuffer> next() {
            try {
                int dataFileOffset = readDataFileOffset();
                dataFile.seek(dataFileOffset);
                return new BaseEntry<>(readByteBuffer(dataFile), readByteBuffer(dataFile));
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

        @Override
        public void close() throws IOException {
            dataFile.close();
            indexesFile.close();
        }

        public void setIndexesFileOffset(long indexesFileOffset) throws IOException {
            indexesFile.seek(indexesFileOffset);
        }

        public long getIndexesFileLength() throws IOException {
            return indexesFile.length();
        }

        private int readDataFileOffset() throws IOException {
            int dataFileOffset = indexesFile.readInt();
            indexesFile.readLine();
            return dataFileOffset;
        }
    }

    private static class CloseableIterator implements Iterator<Entry<ByteBuffer>>, Closeable {

        private final Iterator<Entry<ByteBuffer>> iterator;

        public CloseableIterator(Iterator<Entry<ByteBuffer>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<ByteBuffer> next() {
            return iterator.next();
        }

    }

}
