package ru.mail.polis.dmitreemaximenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Scanner;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private static final String LOG_NAME = "log";
    private static final int BUFFER_SIZE = 1024 * 1024 * 8;
    private final NavigableSet<BaseEntry<byte[]>> data =
            new ConcurrentSkipListSet<>(new RecordNaturalOrderComparator());
    private final EntryWriter diskWriter;
    private final EntryReader diskReader;
    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    public InMemoryDao() throws IOException {
        this(null);
    }

    public InMemoryDao(Config config) throws IOException {
        if (config != null && Files.isDirectory(config.basePath())) {
            Path logPath = config.basePath().resolve(LOG_NAME);

            if (Files.notExists(logPath)) {
                Files.createFile(logPath);
            }

            diskWriter = new EntryWriter(new FileWriter(String.valueOf(logPath),
                    StandardCharsets.UTF_8, true));
            diskReader = new EntryReader(logPath);
        } else {
            diskWriter = null;
            diskReader = null;
        }
    }

    @Override
    public BaseEntry<byte[]> get(byte[] key) throws IOException {
        Iterator<BaseEntry<byte[]>> iterator = get(key, null);
        if (iterator.hasNext()) {
            BaseEntry<byte[]> next = iterator.next();
            if (Arrays.equals(next.key(), key)) {
                return next;
            }
        }

        if (diskReader != null) {
            return diskReader.getByKey(key);
        }

        return null;
    }

    @Override
    public Iterator<BaseEntry<byte[]>> get(byte[] from, byte[] to) {
        if (from == null) {
            return new BorderedIterator(data.iterator(), to);
        }
        return new BorderedIterator(data.tailSet(new BaseEntry<>(from, null), true).iterator(), to);
    }

    @Override
    public void upsert(BaseEntry<byte[]> entry) {
        data.remove(entry);
        data.add(entry);

        if (diskWriter != null) {
            try {
                diskWriter.write(entry);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "The entry is not recorded to disk due to exception!");
            }

        }
    }

    static class BorderedIterator implements Iterator<BaseEntry<byte[]>> {
        private final Iterator<BaseEntry<byte[]>> iterator;
        private final byte[] last;
        private final Integer lastHash;
        private BaseEntry<byte[]> next;

        private BorderedIterator(Iterator<BaseEntry<byte[]>> iterator, byte[] last) {
            this.iterator = iterator;
            next = iterator.hasNext() ? iterator.next() : null;
            this.last = last == null ? null : Arrays.copyOf(last, last.length);
            this.lastHash = last == null ? null : simpleHashCode(last);
        }

        @Override
        public boolean hasNext() {
            return next != null && simpleHashCode(next.key()) != lastHash
                    && !Arrays.equals(next.key(), last);
        }

        @Override
        public BaseEntry<byte[]> next() {
            BaseEntry<byte[]> temp = next;
            next = iterator.hasNext() ? iterator.next() : null;
            return temp;
        }

        private static int simpleHashCode(byte[] last) {
            return (int)last[0] + (int) last[last.length / 2]
                    + (int) last[last.length - 1];
        }
    }

    static class RecordNaturalOrderComparator implements Comparator<BaseEntry<byte[]>> {
        @Override
        public int compare(BaseEntry<byte[]> e1, BaseEntry<byte[]> e2) {
            byte[] key1 = e1.key();
            byte[] key2 = e2.key();
            for (int i = 0; i < Math.min(key1.length, key2.length); i++) {
                if (key1[i] != key2[i]) {
                    return key1[i] - key2[i];
                }
            }
            return key1.length - key2.length;
        }
    }

    static class EntryWriter extends BufferedWriter {
        private static final String SEPARATOR = System.getProperty("line.separator");

        public EntryWriter(Writer out) {
            super(out, BUFFER_SIZE);
        }

        void write(BaseEntry<byte[]> e) throws IOException {
            if (Arrays.toString(e.key()).contains(SEPARATOR)
                    || Arrays.toString(e.value()).contains(SEPARATOR)) {
                throw new IllegalArgumentException("Line separator in the entry");
            }

            String entryRepresentation = new String(e.key(), StandardCharsets.UTF_8)
                    + SEPARATOR
                    + new String(e.value(), StandardCharsets.UTF_8)
                    + SEPARATOR;

            super.write(entryRepresentation);
        }
    }

    static class EntryReader {
        private final Path logPath;

        public EntryReader(Path logPath) {
            this.logPath = logPath;
        }

        public BaseEntry<byte[]> getByKey(byte[] targetKey) throws IOException {
            String targetKeyString = new String(targetKey, StandardCharsets.UTF_8);
            String resultValue = null;

            try (Scanner scanner = new Scanner(logPath, StandardCharsets.UTF_8)) {
                while (scanner.hasNextLine()) {
                    String key = scanner.nextLine();
                    String value = scanner.nextLine();

                    if (key.equals(targetKeyString)) {
                        resultValue = value;
                    }
                }
            }

            return resultValue == null ? null :
                    new BaseEntry<>(targetKeyString.getBytes(StandardCharsets.UTF_8),
                    resultValue.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close() throws IOException {
        if (diskWriter != null) {
            diskWriter.close();
        }
    }
}
