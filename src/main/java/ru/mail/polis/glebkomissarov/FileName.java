package ru.mail.polis.glebkomissarov;

public enum FileName {
    SAVED_DATA("savedData"),
    OFFSETS("offsets"),
    COMPACT_SAVED("compactSavedData"),
    COMPACT_OFFSETS("compactOffsets");

    private final String name;

    FileName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
