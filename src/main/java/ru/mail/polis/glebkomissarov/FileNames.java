package ru.mail.polis.glebkomissarov;

public enum FileNames {
    SAVED_DATA("savedData"),
    OFFSETS("offsets");

    private final String name;

    FileNames(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}