package ru.mail.polis.pavelkovalenko.utils;

import ru.mail.polis.pavelkovalenko.aliases.SSTable;

import java.util.Queue;

public final class DaoUtils {

    private DaoUtils() {
    }

    public static boolean nothingToFlush(Queue<SSTable> sstablesForWrite) {
        return sstablesForWrite.isEmpty() || sstablesForWrite.peek().isEmpty();
    }

    public static boolean thereIsSmthToFlush(Queue<SSTable> sstablesForWrite) {
        return !nothingToFlush(sstablesForWrite);
    }
}
