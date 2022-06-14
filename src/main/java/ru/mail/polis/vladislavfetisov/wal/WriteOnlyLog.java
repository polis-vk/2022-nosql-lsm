package ru.mail.polis.vladislavfetisov.wal;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.Entries;
import ru.mail.polis.vladislavfetisov.MemorySegments;
import ru.mail.polis.vladislavfetisov.Utils;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WriteOnlyLog {
    private final AtomicReference<LogState> state = new AtomicReference<>(new LogState());
    private final AtomicInteger putCount = new AtomicInteger();
    private final MemorySegment recordsFile;
    private final MemorySegment positionsFile;


    private final Path tableName;
    private final Path positionsName;

    public Path getTableName() {
        return tableName;
    }

    public Path getPositionsName() {
        return positionsName;
    }

    public AtomicInteger getPutCount() {
        return putCount;
    }

    public AtomicReference<LogState> getState() {
        return state;
    }

    public long size() {
        return recordsFile.byteSize();
    }

    public WriteOnlyLog(Path tableName, ResourceScope scope, long length) throws IOException {
        this.tableName = tableName;
        this.positionsName = Path.of(tableName + SSTable.INDEX);
        Utils.newFile(tableName);
        Utils.newFile(positionsName);
        this.recordsFile = MemorySegments.map(tableName, length, FileChannel.MapMode.READ_WRITE, scope);
        long maxOffsetsLength = ((length - Long.BYTES) / Entries.MIN_LENGTH) * WAL.POSITION_LENGTH;
        this.positionsFile =
                MemorySegments.map(positionsName, maxOffsetsLength, FileChannel.MapMode.READ_WRITE, scope);
    }

    public void putEntry(long offset, Entry<MemorySegment> entry) {
        MemorySegments.writeEntry(recordsFile, offset, entry);
    }

    public void setPositionsSize(long value) {
        MemoryAccess.setLongAtOffset(recordsFile, 0, value);
    }

    public void putPosition(long offset, long value) {
        MemoryAccess.setLongAtOffset(positionsFile, offset, value);
    }

    public void putPositionChecksum(long offset, long checksum) {
        MemoryAccess.setLongAtOffset(positionsFile, offset, checksum);
    }

    public boolean flush(LogState localState) {
        boolean success = localState.startFlush.compareAndSet(false, true);
        if (!success) {
            return false;
        }
        this.state.getAndSet(new LogState());
        WAL.LOGGER.info("Try to sync local state {}", localState.hashCode());
//        recordsFile.force();
//        positionsFile.force();
        localState.isFlushed = true;
        localState.lock.lock();
        try {
            localState.flushCondition.signalAll();
        } finally {
            localState.lock.unlock();
        }
        return true;
    }

    public static class LogState {
        private final Lock lock = new ReentrantLock();
        private final Condition flushCondition = lock.newCondition();
        private final AtomicBoolean startFlush = new AtomicBoolean();
        private volatile boolean isFlushed;

        public Lock getLock() {
            return lock;
        }

        public boolean isFlushed() {
            return isFlushed;
        }

        public Condition getFlushCondition() {
            return flushCondition;
        }
    }
}
