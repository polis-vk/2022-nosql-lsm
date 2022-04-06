package ru.mail.polis.glebkomissarov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MyMemoryDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private static final MemorySegment FIRST_KEY = MemorySegment.ofArray(new byte[]{});
    private static final String CMP_DIR = "COMPACT DIRECTORY";
    private static final int PRIORITY = 0;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentSkipListMap<MemorySegment, BaseEntry<MemorySegment>> data = new ConcurrentSkipListMap<>(
            Comparator::compare
    );
    private final Path basePath;
    private final FileWorker fileWorker;

    private boolean compacted;

    public MyMemoryDao(Config config) {
        basePath = config.basePath();
        fileWorker = new FileWorker();
        fileWorker.load(basePath);
    }

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) throws IOException {
        lock.readLock().lock();
        try {
            MemorySegment newFrom = from;
            if (from == null) {
                newFrom = FIRST_KEY;
            }

            Iterator<BaseEntry<MemorySegment>> memoryIterator;
            if (to == null) {
                memoryIterator = data.tailMap(newFrom).values().iterator();
            } else {
                memoryIterator = data.subMap(newFrom, to).values().iterator();
            }

            if (fileWorker.fileCount() <= 0) {
                return memoryIterator;
            }

            List<PeekIterator> iterators = fileWorker.findEntries(from, to);
            iterators.add(new PeekIterator(memoryIterator, fileWorker.fileCount() + 1));
            return new RangeIterator(iterators);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) throws IOException {
         lock.readLock().lock();
        try {
            BaseEntry<MemorySegment> result = data.get(key);
            if (result == null) {
                result = fileWorker.findEntry(key);
            }

            if (result != null && result.value() == null) {
                return null;
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        lock.writeLock().lock();
        try {
            if (fileWorker.fileCount() == 0 && entry.value() == null) {
                data.remove(entry.key());
                return;
            }
            data.put(entry.key(), entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        fileWorker.writeEntries(data.values(), basePath, compacted, FileName.SAVED_DATA, FileName.OFFSETS);
        if (compacted) {
            compact();
        }
    }

    @Override
    public synchronized void compact() throws IOException {
        compacted = true;
        if (fileWorker.fileCount() < 2) {
            return;
        }

        // Если выполнился, то за основу всё равно берутся старые файлы
        CollapseTogether.createCompact(fileWorker,
                basePath.resolve(CMP_DIR),
                new RangeIterator(fileWorker.findEntries(null, null)));
        // Если хотя бы частично выполнился, то при открытии дао старые данные
        // будут удалены, а чтение будет производиться только из новых файлов
        Path offset = basePath.resolve(CMP_DIR).resolve(FileName.COMPACT_OFFSETS.getName() + PRIORITY);
        Path savedData = basePath.resolve(CMP_DIR).resolve(FileName.COMPACT_SAVED.getName() + PRIORITY);
        CollapseTogether.moveFile(offset, basePath.resolve(FileName.COMPACT_OFFSETS.getName() + PRIORITY));
        CollapseTogether.moveFile(savedData, basePath.resolve(FileName.COMPACT_SAVED.getName() + PRIORITY));
        // Удаляет старые файлы, влияет только на то, когда будут удалены старые данные
        CollapseTogether.removeOld(basePath);
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (data.isEmpty()) {
                return;
            }
            flush();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
