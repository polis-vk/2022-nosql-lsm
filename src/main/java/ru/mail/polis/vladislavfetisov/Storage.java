package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public record Storage(Memory memory, Memory readOnlyMemory, List<SSTable> ssTables, Config config) {

    public Storage beforeFlush() {
        if (readOnlyMemory != Memory.EMPTY_MEMORY) {
            throw new IllegalStateException("Already flushing");
        }
        return new Storage(Memory.getNewMemory(config.flushThresholdBytes()),
                this.memory,
                this.ssTables,
                config);
    }

    public Storage afterFlush() {
        if (readOnlyMemory == Memory.EMPTY_MEMORY) {
            throw new IllegalStateException("Wasn't flushing");
        }
        return new Storage(this.memory, Memory.EMPTY_MEMORY, this.ssTables, config);
    }

    public Storage updateSSTables(List<SSTable> newTables) {
        return new Storage(this.memory, this.readOnlyMemory, newTables, config);
    }

    public boolean isFlushing() {
        return readOnlyMemory != Memory.EMPTY_MEMORY;
    }

    public static class Memory {
        private final long sizeLimit;
        private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> delegate;
        private final AtomicLong size = new AtomicLong();
        private final AtomicBoolean oversize = new AtomicBoolean();

        public static final Memory EMPTY_MEMORY = getNewMemory(-1);

        private Memory(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> delegate, long sizeThreshold) {
            this.sizeLimit = sizeThreshold;
            this.delegate = delegate;
        }

        public static Memory getNewMemory(long sizeThreshold) {
            return new Memory(new ConcurrentSkipListMap<>(Utils::compareMemorySegments), sizeThreshold);
        }

        public boolean put(MemorySegment key, Entry<MemorySegment> value) {
            if (sizeLimit == -1) {
                throw new UnsupportedOperationException("ReadOnly memory");
            }
            Entry<MemorySegment> previous = delegate.put(key, value);
            long delta = Utils.sizeOfEntry(value);
            if (previous != null) {
                delta -= Utils.sizeOfEntry(previous);
            }
            long newSize = size.addAndGet(delta);
            if (newSize > sizeLimit) {
                return !oversize.getAndSet(true);
            }
            return false;
        }

        public AtomicBoolean isOversize() {
            return oversize;
        }

        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        public Collection<Entry<MemorySegment>> values() {
            return delegate.values();
        }

        public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
            if (from == null && to == null) {
                return delegate.values().iterator();
            }
            return subMap(from, to).values().iterator();
        }

        public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap(
                MemorySegment from,
                MemorySegment to) {
            if (from == null) {
                return delegate.headMap(to);
            }
            if (to == null) {
                return delegate.tailMap(from);
            }
            return delegate.subMap(from, to);
        }
    }
}
