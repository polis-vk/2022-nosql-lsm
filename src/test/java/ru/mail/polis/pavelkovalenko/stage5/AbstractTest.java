package ru.mail.polis.pavelkovalenko.stage5;

public interface AbstractTest {

    int FLUSH_TRESHOLD = 1 << 20; // 1 MB
    int ENTRY_SIZE = EntrySizeRealization.ONE.getEntrySize();
    int N_ENTRIES_FOR_AUTOFLUSH = (int) Math.ceil((double) FLUSH_TRESHOLD / ENTRY_SIZE);
    int N_ENTRIES_FOR_ABSENT_AUTOFLUSH = N_ENTRIES_FOR_AUTOFLUSH - 1;
}
