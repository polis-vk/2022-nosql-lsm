package ru.mail.polis.pavelkovalenko;

import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;
import ru.mail.polis.pavelkovalenko.iterators.MergeIterator;
import ru.mail.polis.pavelkovalenko.visitors.CompactVisitor;
import ru.mail.polis.pavelkovalenko.visitors.ConfigVisitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LSMDao implements Dao<ByteBuffer, Entry<ByteBuffer>> {

    private final ConcurrentNavigableMap<ByteBuffer, Entry<ByteBuffer>> memorySSTable = new ConcurrentSkipListMap<>();
    private final NavigableMap<Integer /*priority*/, PairedFiles> sstables = new TreeMap<>();
    private final Config config;
    private final Serializer serializer;

    public LSMDao(Config config) throws IOException {
        this.config = config;
        this.serializer = new Serializer(sstables, config);
        Files.walkFileTree(config.basePath(), new ConfigVisitor(sstables, serializer));
    }

    @Override
    public Iterator<Entry<ByteBuffer>> get(ByteBuffer from, ByteBuffer to) throws IOException {
        return new MergeIterator(from, to, serializer, memorySSTable, sstables);
    }

    @Override
    public void upsert(Entry<ByteBuffer> entry) {
        memorySSTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        serializer.write(memorySSTable.values().iterator());
    }

    @Override
    public void close() throws IOException {
        if (memorySSTable.isEmpty()) {
            return;
        }

        flush();
        memorySSTable.clear();
    }

    @Override
    public void compact() throws IOException {
        if (memorySSTable.isEmpty() && sstables.isEmpty()) {
            return;
        }

        Iterator<Entry<ByteBuffer>> mergeIterator = get(null, null);
        if (!mergeIterator.hasNext()) {
            return;
        }
        serializer.write(mergeIterator);
        Files.walkFileTree(config.basePath(), new CompactVisitor(sstables.lastEntry().getValue(), config));
        memorySSTable.clear();
    }

}
