package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.pavelkovalenko.aliases.SSTable;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class DaoUtils {

    private DaoUtils() {
    }

    public static boolean nothingToFlush(Queue<SSTable> sstablesForWrite) {
        return sstablesForWrite.isEmpty() || sstablesForWrite.peek().isEmpty();
    }

    public static boolean thereIsSmthToFlush(Queue<SSTable> sstablesForWrite) {
        return !nothingToFlush(sstablesForWrite);
    }

    public static boolean noNeedsInCompact(AtomicInteger sstablesSize, AtomicBoolean filesAppearedSinceLastCompact) {
        return sstablesSize.get() == 0 || !filesAppearedSinceLastCompact.get();
    }
}
