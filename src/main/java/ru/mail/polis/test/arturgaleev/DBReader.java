package ru.mail.polis.test.arturgaleev;

import ru.mail.polis.BaseEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Stream;

public class DBReader implements AutoCloseable {
    private final String DB_FILES_EXTENSION = ".txt";
    private final List<FileDBReader> fileReaders;

    public DBReader(Path DBDirectoryPath) throws IOException {
        if (!Files.isDirectory(DBDirectoryPath)) {
            throw new NotDirectoryException(DBDirectoryPath + " is not a directory with DB files");
        }
        fileReaders = getFileDBReaders(DBDirectoryPath);
    }

    private List<FileDBReader> getFileDBReaders(Path DBDirectoryPath) throws IOException {
        final List<FileDBReader> fileReaders;
        try (Stream<Path> files = Files.list(DBDirectoryPath)) {
            fileReaders = files
                    .filter(path -> path.toString().endsWith(DB_FILES_EXTENSION))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .map(path -> {
                        try {
                            return new FileDBReader(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }).toList();
            int i = 0;
            for (FileDBReader reader : fileReaders) {
                reader.setFileID(i++);
            }
        }
        return fileReaders;
    }

//    public

    public PeakingIterator get(ByteBuffer from, ByteBuffer to) {
        List<FileDBReader.PeakingIterator> iterators = new ArrayList<>(fileReaders.size());
        for (FileDBReader reader : fileReaders) {
            FileDBReader.PeakingIterator fromToIterator = reader.getFromToIterator(from, to);
            if (fromToIterator.hasNext()) {
                iterators.add(fromToIterator);
            }
        }
        return new PeakingIterator(iterators);
    }

    public BaseEntry<ByteBuffer> get(ByteBuffer key) {
        for (int i = fileReaders.size() - 1; i >= 0; i--) {
            BaseEntry<ByteBuffer> entryByKey = fileReaders.get(i).getEntryByKey(key);
            if (entryByKey != null) {
                if (entryByKey.value() != null) {
                    return entryByKey;
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        for (FileDBReader fileReader : fileReaders) {
            fileReader.close();
        }
    }

    public class PeakingIterator implements Iterator<BaseEntry<ByteBuffer>> {
        private final PriorityBlockingQueue<FileDBReader.PeakingIterator> iteratorsQueue;
        private BaseEntry<ByteBuffer> current = null;

        public PeakingIterator(List<FileDBReader.PeakingIterator> iterators) {
            if (iterators.isEmpty()) {
                iteratorsQueue = new PriorityBlockingQueue<>();
            } else {
                iteratorsQueue = new PriorityBlockingQueue<>(iterators.size(),
                        Comparator.comparing(
                                        (FileDBReader.PeakingIterator o) -> o.peek().key())
                                .thenComparing(FileDBReader.PeakingIterator::getFileId, Comparator.reverseOrder())
                );
                iteratorsQueue.addAll(iterators);
            }
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                try {
                    current = next();
                } catch (IndexOutOfBoundsException e) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public BaseEntry<ByteBuffer> next() {
            if (current != null) {
                String str = toString(current.key()) + "" + toString(current.value());
                BaseEntry<ByteBuffer> prev = current;
                current = null;
                return prev;
            }
            if (iteratorsQueue.isEmpty()) {
                throw new IndexOutOfBoundsException();
            }

            BaseEntry<ByteBuffer> entry = getNotRemovedDeletedElement();
            //String str = toString(entry.key()) + "" + toString(entry.value());

            if (entry.value() == null) {
                throw new IndexOutOfBoundsException();
            }
            return entry;
        }

        public String toString(ByteBuffer in) {
            ByteBuffer data = in.asReadOnlyBuffer();
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private BaseEntry<ByteBuffer> getNotRemovedDeletedElement() {
            FileDBReader.PeakingIterator iterator = iteratorsQueue.poll();
            BaseEntry<ByteBuffer> entry = iterator.next();
            if (iterator.hasNext()) {
                iteratorsQueue.put(iterator);
            }
            removeElementsWithKey(entry.key());

            while (!iteratorsQueue.isEmpty() && entry.value() == null) {
                iterator = iteratorsQueue.poll();
                entry = iterator.next();
                if (iterator.hasNext()) {
                    iteratorsQueue.put(iterator);
                }
                removeElementsWithKey(entry.key());
            }
            return entry;
        }

        private void removeElementsWithKey(ByteBuffer lastKey) {
            while (!iteratorsQueue.isEmpty() && lastKey.equals(iteratorsQueue.peek().peek().key())) {
                FileDBReader.PeakingIterator poll = iteratorsQueue.poll();
                if (poll.hasNext()) {
                    BaseEntry<ByteBuffer> entry = poll.next();
                    if (poll.hasNext()) {
                        iteratorsQueue.put(poll);
                    }
                }
            }
        }

        public BaseEntry<ByteBuffer> peek() {
            if (!hasNext()) {
                throw new IndexOutOfBoundsException();
            }
            if (current == null) {
                current = next();
            }
            return current;
        }
    }
}
