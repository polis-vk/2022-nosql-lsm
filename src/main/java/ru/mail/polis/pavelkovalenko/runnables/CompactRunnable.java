package ru.mail.polis.pavelkovalenko.runnables;

import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.Serializer;
import ru.mail.polis.pavelkovalenko.dto.FileMeta;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.utils.DaoUtils;
import ru.mail.polis.pavelkovalenko.utils.FileUtils;
import ru.mail.polis.pavelkovalenko.visitors.CompactVisitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class CompactRunnable implements Runnable {

    private final Config config;
    private final AtomicInteger sstablesSize;
    private final AtomicBoolean filesAppearedSinceLastCompact;
    private final Serializer serializer;
    private final Lock interThreadedLock;

    public CompactRunnable(Config config, AtomicInteger sstablesSize, AtomicBoolean filesAppearedSinceLastCompact,
                   Serializer serializer, Lock interThreadedLock) {
        this.config = config;
        this.sstablesSize = sstablesSize;
        this.filesAppearedSinceLastCompact = filesAppearedSinceLastCompact;
        this.serializer = serializer;
        this.interThreadedLock = interThreadedLock;
    }

    @Override
    public void run() {
        if (DaoUtils.noNeedsInCompact(sstablesSize, filesAppearedSinceLastCompact)) {
            return;
        }

        Iterator<Entry<ByteBuffer>> mergeIterator;
        try {
            interThreadedLock.lock();
            try {
                if (DaoUtils.noNeedsInCompact(sstablesSize, filesAppearedSinceLastCompact)) {
                    return;
                }

                if (sstablesSize.get() == 1) {
                    FileMeta meta = serializer.readMeta(serializer.get(1).dataFile());
                    if (!serializer.hasTombstones(meta)) {
                        return;
                    }
                }

                mergeIterator
                        = new MergeIterator(null, null, serializer, Collections.EMPTY_LIST, sstablesSize);
                if (!mergeIterator.hasNext()) {
                    return;
                }
            } finally {
                interThreadedLock.unlock();
            }

            Path compactDataFile;
            Path compactIndexesFile;
            int iteration = 0;
            interThreadedLock.lock();
            try {
                do {
                    ++iteration;
                    compactDataFile
                            = FileUtils.getFilePath(FileUtils.getCompactDataFilename(iteration), config);
                    compactIndexesFile
                            = FileUtils.getFilePath(FileUtils.getCompactIndexesFilename(iteration), config);
                } while (Files.exists(compactDataFile));
                Files.createFile(compactDataFile);
                Files.createFile(compactIndexesFile);
            } finally {
                interThreadedLock.unlock();
            }

            serializer.write(mergeIterator, compactDataFile, compactIndexesFile);
            interThreadedLock.lock();
            try {
                Files.walkFileTree(config.basePath(),
                        new CompactVisitor(config, compactDataFile, compactIndexesFile, sstablesSize, serializer));
                filesAppearedSinceLastCompact.set(false);
            } finally {
                interThreadedLock.unlock();
            }
        } catch (IOException | ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
