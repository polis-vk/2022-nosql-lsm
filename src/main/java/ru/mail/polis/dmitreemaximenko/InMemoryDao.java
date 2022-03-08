package ru.mail.polis.dmitreemaximenko;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class InMemoryDao implements Dao<byte[], BaseEntry<byte[]>> {
    private static final String LOG_NAME = "log";
    private final NavigableSet<BaseEntry<byte[]>> data =
            new ConcurrentSkipListSet<>(new RecordNaturalOrderComparator());
    private final EntryWriter diskWriter;
    private final EntryReader diskReader;

    public InMemoryDao() throws IOException {
        this(null);
    }

    public InMemoryDao(Config config) throws IOException {
        if (config != null && Files.isDirectory(config.basePath())) {
            Path logPath = Path.of(config.basePath() + System.getProperty("file.separator") + LOG_NAME);

            if (Files.notExists(logPath)) {
                Files.createFile(logPath);
            }

            diskWriter = new EntryWriter(new FileWriter(String.valueOf(logPath), StandardCharsets.UTF_8));
            diskReader = new EntryReader(new FileInputStream(String.valueOf(logPath)));
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
                System.err.println("The entry is not recorded to disk due to exception!");
            }

        }
    }

    static class BorderedIterator implements Iterator<BaseEntry<byte[]>> {
        private final Iterator<BaseEntry<byte[]>> iterator;
        private final byte[] last;
        private BaseEntry<byte[]> next;

        private BorderedIterator(Iterator<BaseEntry<byte[]>> iterator, byte[] last) {
            this.iterator = iterator;
            next = iterator.hasNext() ? iterator.next() : null;
            this.last = last == null ? null : Arrays.copyOf(last, last.length);
        }

        @Override
        public boolean hasNext() {
            return next != null && !Arrays.equals(next.key(), last);
        }

        @Override
        public BaseEntry<byte[]> next() {
            BaseEntry<byte[]> temp = next;
            next = iterator.hasNext() ? iterator.next() : null;
            return temp;
        }
    }

    static class RecordNaturalOrderComparator implements Comparator<BaseEntry<byte[]>> {
        @Override
        public int compare(BaseEntry<byte[]> e1, BaseEntry<byte[]> e2) {
            byte[] key1 = e1.key();
            byte[] key2 = e2.key();
            for (int i = 0; i < Math.min(key1.length, key2.length); ++i) {
                if (key1[i] != key2[i]) {
                    return key1[i] - key2[i];
                }
            }
            return key1.length - key2.length;
        }
    }

    static class EntryWriter extends BufferedWriter {
        public EntryWriter(Writer out) {
            super(out);
        }

        void write(BaseEntry<byte[]> e) throws IOException {
            super.write(new String(e.key(), StandardCharsets.UTF_8));
            super.write(System.getProperty("line.separator"));
            super.write(new String(e.value(), StandardCharsets.UTF_8));
            super.write(System.getProperty("line.separator"));
        }
    }

    static class EntryReader {
        private final FileInputStream fileInputStream;

        public EntryReader(FileInputStream fileInputStream) {
            this.fileInputStream = fileInputStream;
        }

        public BaseEntry<byte[]> getByKey(byte[] targetKey) throws IOException {
            fileInputStream.getChannel().position(0);
            String targetKeyString = Arrays.toString(targetKey);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream,
                    StandardCharsets.UTF_8));
            while (true) {
                String key = bufferedReader.readLine();
                String value = bufferedReader.readLine();

                if (key == null || value == null) {
                    return null;
                }

                if (key.equals(targetKeyString)) {
                    return new BaseEntry<>(key.getBytes(StandardCharsets.UTF_8),
                            value.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (diskWriter != null) {
            diskWriter.close();
        }
    }
}
