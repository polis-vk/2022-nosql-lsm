package ru.mail.polis.pavelkovalenko.stage5;

public enum EntrySizeRealization {

    ONE(/*data*/ 2 /*key and value*/ * (Integer.BYTES + 11 /*keysize and valuesize*/)
            + /*index*/ Integer.BYTES),
    TWO(/*data*/ 2 /*key and value*/ * (Integer.BYTES + 11 /*keysize and valuesize*/ * Character.BYTES)
            + /*index*/ Integer.BYTES),
    THREE(/*data*/ 2 /*key and value*/ * (Long.BYTES + 11 /*keysize and valuesize*/)
            + /*index*/ Long.BYTES),
    FOUR(/*data*/ 2 /*key and value*/ * (Long.BYTES + 11 /*keysize and valuesize*/ * Character.BYTES)
            + /*index*/ Long.BYTES);

    private final int entrySize;

    EntrySizeRealization(int entrySize) {
        this.entrySize = entrySize;
    }

    public int getEntrySize() {
        return this.entrySize;
    }
}
