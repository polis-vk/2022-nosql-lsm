package ru.mail.polis.vladislavfetisov;

import jdk.incubator.foreign.MemorySegment;
import ru.mail.polis.Config;
import ru.mail.polis.Entry;
import ru.mail.polis.vladislavfetisov.iterators.CustomIterators;
import ru.mail.polis.vladislavfetisov.iterators.PeekingIterator;
import ru.mail.polis.vladislavfetisov.lsm.LsmDao;
import ru.mail.polis.vladislavfetisov.lsm.SSTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class Utils {

    private Utils() {

    }

    public static long binarySearch(MemorySegment key,
                                    MemorySegment mapFile,
                                    MemorySegment mapIndex) {
        long l = 0;
        long rightBound = mapIndex.byteSize() / Long.BYTES;
        long r = rightBound - 1;
        while (l <= r) {
            long middle = (l + r) >>> 1;
            Entry<MemorySegment> middleEntry = MemorySegments.getByIndex(mapFile, mapIndex, middle);
            int res = MemorySegments.compareMemorySegments(middleEntry.key(), key);
            if (res == 0) {
                return middle;
            } else if (res < 0) {
                l = middle + 1;
            } else {
                r = middle - 1;
            }
        }
        if (r == -1) {
            return -(l + 1);
        }
        return l;
    }

    public static void rename(Path source, Path target) throws IOException {
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    public static Path withSuffix(Path path, String suffix) {
        return path.resolveSibling(path.getFileName() + suffix);
    }

    public static void deleteTablesToIndex(List<SSTable> tableList, int toIndex) throws IOException {
        for (int i = 0; i < toIndex; i++) {
            SSTable table = tableList.get(i);
            Files.deleteIfExists(table.getTableName());
            Files.deleteIfExists(table.getIndexName());
        }
    }

    public static String removeSuffix(String source, String suffix) {
        return source.substring(0, source.length() - suffix.length());
    }

    public static Iterator<Entry<MemorySegment>> tablesRange(
            MemorySegment from, MemorySegment to, List<SSTable> tables) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(tables.size());
        for (SSTable table : tables) {
            iterators.add(table.range(from, to));
        }
        return CustomIterators.merge(iterators);
    }

    public static long getLastTableNum(List<SSTable> ssTables) {
        String lastTable = ssTables.get(ssTables.size() - 1).getTableName().getFileName().toString();
        if (lastTable.endsWith(SSTable.COMPACTED)) {
            lastTable = Utils.removeSuffix(lastTable, SSTable.COMPACTED);
        }
        return Long.parseLong(lastTable) + 1;
    }

    public static Iterator<Entry<MemorySegment>> tablesFilteredFullRange(List<SSTable> fixed) {
        Iterator<Entry<MemorySegment>> discIterator = Utils.tablesRange(null, null, fixed);
        PeekingIterator<Entry<MemorySegment>> iterator = new PeekingIterator<>(discIterator);
        return CustomIterators.skipTombstones(iterator);
    }

    public static void shutdownExecutor(ExecutorService service) {
        service.shutdown();
        try {
            if (!service.awaitTermination(1, TimeUnit.HOURS)) {
                throw new IllegalStateException("Cant await termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LsmDao.LOGGER.error("Cant await termination", e);
        }
    }

    public static void newFile(Path name) throws IOException {
        Files.newByteChannel(name,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    public static Integer getTableNum(Path path) {
        return Integer.parseInt(path.getFileName().toString());
    }

    public static Path nextTable(String name, Config config) {
        return config.basePath().resolve(name);
    }
}
