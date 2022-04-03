package ru.mail.polis.kirillpobedonostsev;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class PersistenceDao implements Dao<ByteBuffer, BaseEntry<ByteBuffer>> {

    public static final int NULL_VALUE_LENGTH = -1;
    public static final int FILE_HEADER_SIZE = Long.BYTES + Integer.BYTES;

    private static final long CURRENT_FILE_VERSION = 0;
    private static final int COMPACTED_TMP_FILE_NUMBER = 0;
    private static final String FILE_EXTENSION = ".dat";
    private static final String FILE_TMP_EXTENSION = ".tmp";
    private static final String DATA_FILE_PREFIX = "data";

    private final ConcurrentNavigableMap<ByteBuffer, BaseEntry<ByteBuffer>> map =
            new ConcurrentSkipListMap<>(ByteBuffer::compareTo);
    private final List<MappedByteBuffer> files;
    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public PersistenceDao(Config config) throws IOException {
        this.config = config;
        checkTmpFile();
        files = new ArrayList<>();
        boolean fileExist = true;
        for (int i = 0; fileExist; i++) {
            try (FileChannel channel = FileChannel.open(getFilePath(i))) {
                MappedByteBuffer mappedDataFile =
                        channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
                files.add(mappedDataFile);
            } catch (NoSuchFileException e) {
                fileExist = false;
            }
        }
    }

    private void checkTmpFile() throws IOException {
        Path tmpFileName = getTmpFilePath(COMPACTED_TMP_FILE_NUMBER);
        if (Files.exists(tmpFileName)) {
            cleanupDirectory(tmpFileName);
            Files.move(tmpFileName, getFilePath(COMPACTED_TMP_FILE_NUMBER), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    @Override
    public Iterator<BaseEntry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        lock.readLock().lock();
        try {
            List<PeekingIterator<BaseEntry<ByteBuffer>>> iteratorsList =
                    new ArrayList<>(files.size() + 1);
            iteratorsList.add(new PeekingIterator<>(getInMemoryIterator(from, to), files.size()));
            for (int i = files.size() - 1; i >= 0; i--) {
                iteratorsList.add(new PeekingIterator<>(getFileIterator(from, to, i), i));
            }
            return new MergeIterator(iteratorsList);
        } finally {
            lock.readLock().unlock();
        }
    }

    private FileIterator getFileIterator(ByteBuffer from, ByteBuffer to, int fileNumber) {
        ByteBuffer mappedDataFile = files.get(fileNumber);
        int fromOffset = from == null ? 0 : binarySearch(mappedDataFile, from);
        int toOffset = binarySearch(mappedDataFile, to);
        return new FileIterator(mappedDataFile, fromOffset, toOffset);
    }

    private Iterator<BaseEntry<ByteBuffer>> getInMemoryIterator(ByteBuffer from, ByteBuffer to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to, false).values().iterator();
        } else if (to == null) {
            return map.tailMap(from, true).values().iterator();
        }
        return map.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(BaseEntry<ByteBuffer> entry) {
        lock.readLock().lock();
        try {
            map.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            Path tempFile = getTmpFilePath(files.size());
            MappedByteBuffer dataPage = save(() -> map.values().iterator(), tempFile);
            if (dataPage == null) {
                return;
            }
            files.add(dataPage);
            Files.move(tempFile, getFilePath(files.size() - 1),
                    StandardCopyOption.ATOMIC_MOVE);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int writeItem(ByteBuffer file, ByteBuffer item, int offset) {
        if (item == null) {
            file.putInt(offset, NULL_VALUE_LENGTH);
            return Integer.BYTES;
        }
        int written = item.remaining() + Integer.BYTES;
        file.putInt(offset, item.remaining());
        file.put(offset + Integer.BYTES, item, 0, item.remaining());
        return written;
    }

    private Path getFilePath(int number) {
        return config.basePath().resolve(DATA_FILE_PREFIX + number + FILE_EXTENSION);
    }

    private Path getTmpFilePath(int number) {
        return config.basePath().resolve(DATA_FILE_PREFIX + number + FILE_TMP_EXTENSION);
    }

    private static int binarySearch(ByteBuffer dataFile, ByteBuffer key) {
        long fileVersion = dataFile.getLong(0);
        if (fileVersion != CURRENT_FILE_VERSION) {
            throw new IllegalStateException("Unknown version: " + fileVersion);
        }
        int recordsCount = dataFile.getInt(Long.BYTES);

        if (key == null) {
            return recordsCount;
        }

        int low = 0;
        int high = recordsCount - 1;
        int offset;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            offset = dataFile.getInt(FILE_HEADER_SIZE + mid * Integer.BYTES);
            int keySize = dataFile.getInt(offset);
            ByteBuffer readKey = dataFile.slice(offset + Integer.BYTES, keySize);
            int compareResult = readKey.compareTo(key);
            if (compareResult > 0) {
                high = mid - 1;
            } else if (compareResult < 0) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return low;
    }

    private MappedByteBuffer save(Supplier<Iterator<BaseEntry<ByteBuffer>>> iteratorSupplier, Path tempFile)
            throws IOException {
        Iterator<BaseEntry<ByteBuffer>> entryIterator = iteratorSupplier.get();
        if (!entryIterator.hasNext()) {
            return null;
        }
        int size = 0;
        int count = 0;
        BaseEntry<ByteBuffer> entry;
        while (entryIterator.hasNext()) {
            entry = entryIterator.next();
            size += entry.key().remaining();
            if (entry.value() != null) {
                size += entry.value().remaining();
            }
            count++;
        }
        int offset = FILE_HEADER_SIZE + count * Integer.BYTES;
        size += offset;
        size += count * 2 * Integer.BYTES;

        MappedByteBuffer dataPage;
        try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            dataPage = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        }

        dataPage.putLong(CURRENT_FILE_VERSION);
        dataPage.putInt(count);
        int index = 0;
        entryIterator = iteratorSupplier.get();
        while (entryIterator.hasNext()) {
            entry = entryIterator.next();
            dataPage.putInt(FILE_HEADER_SIZE + index * Integer.BYTES, offset);
            offset += writeItem(dataPage, entry.key(), offset);
            offset += writeItem(dataPage, entry.value(), offset);
            index++;
        }
        dataPage.force();
        return dataPage;
    }

    @Override
    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            Path tmpFileName = getTmpFilePath(COMPACTED_TMP_FILE_NUMBER);
            MappedByteBuffer dataPage = save(() -> {
                try {
                    return all();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, tmpFileName);
            if (dataPage == null) {
                return;
            }
            files.clear();
            files.add(dataPage);
            map.clear();
            cleanupDirectory(tmpFileName);
            Files.move(tmpFileName, getFilePath(COMPACTED_TMP_FILE_NUMBER), StandardCopyOption.ATOMIC_MOVE);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void cleanupDirectory(Path exclude) throws IOException {
        Files.walkFileTree(config.basePath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!file.equals(exclude)) {
                    Files.delete(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
