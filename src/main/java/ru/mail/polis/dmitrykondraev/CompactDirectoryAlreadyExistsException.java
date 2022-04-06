package ru.mail.polis.dmitrykondraev;

import java.nio.file.Path;

public class CompactDirectoryAlreadyExistsException extends IllegalStateException {
    // Note: not meant to serialize this exception
    private final transient Path directory;

    public CompactDirectoryAlreadyExistsException(Path directory) {
        super();
        this.directory = directory;
    }

    public CompactDirectoryAlreadyExistsException(String s, Path directory) {
        super(s);
        this.directory = directory;
    }

    public Path directory() {
        return directory;
    }
}
