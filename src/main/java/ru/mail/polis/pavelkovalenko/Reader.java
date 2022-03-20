package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Entry;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

public class Reader {

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data;
    private final NavigableMap<Integer, Entry<Path>> pathsToPairedFiles;

    public Reader(ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> data,
                  NavigableMap<Integer, Entry<Path>> pathsToPairedFiles) {
        this.data = data;
        this.pathsToPairedFiles = pathsToPairedFiles.descendingMap();
    }

    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws FileNotFoundException {
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

        private final ByteBuffer from;
        private final ByteBuffer to;
        private final Map<ByteBuffer, Entry<ByteBuffer>> mergedData = new TreeMap<>();
        private final Iterator<Map.Entry<ByteBuffer, Entry<ByteBuffer>>> mergedDataIterator = mergedData.entrySet().iterator();
        private final List<CloseableIterator> iterators = new ArrayList<>();
        private final List<Entry<ByteBuffer>> lastEntries = new ArrayList<>();

        public MergeIterator(ByteBuffer from, ByteBuffer to) throws FileNotFoundException {
            this.from = from;
            this.to = to;

            iterators.add(new CloseableIterator(data.values().iterator()));
            for (Map.Entry<Integer, Entry<Path>> pathToPairedFiles: pathsToPairedFiles.entrySet()) {
                Path pathToDataFile = pathToPairedFiles.getValue().key();
                Path pathToIndexesFile = pathToPairedFiles.getValue().value();
                iterators.add(new FileIterator(pathToDataFile, pathToIndexesFile));
            }
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = false;
            for (Iterator<Entry<ByteBuffer>> iterator: iterators) {
                hasNext |= iterator.hasNext();
            }
            return hasNext;
        }

        @Override
        public Entry<ByteBuffer> next() {
            return mergedDataIterator.next().getValue();
        }

        @Override
        public void close() {
            try {
                for (CloseableIterator closeableIterator: iterators) {
                    closeableIterator.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private class FileIterator extends CloseableIterator {

        private final RandomAccessFile dataFile;
        private final RandomAccessFile indexesFile;

        public FileIterator(Path dataFilePath, Path indexesFilePath) throws FileNotFoundException {
            super(null);
            dataFile = new RandomAccessFile(dataFilePath.toString(), "r");
            indexesFile = new RandomAccessFile(indexesFilePath.toString(), "r");
        }

        @Override
        public boolean hasNext() {
            try {
                return dataFile.getFilePointer() < dataFile.length();
            } catch (IOException e) {
                return false;
            }
        }

        @Override
        public Entry<ByteBuffer> next() {
            try {
                int dataFileOffset = readDataFileOffset();
                dataFile.seek(dataFileOffset);
                return new BaseEntry<>(readByteBuffer(dataFile), readByteBuffer(dataFile));
            } catch (IOException e) {
                return null;
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
