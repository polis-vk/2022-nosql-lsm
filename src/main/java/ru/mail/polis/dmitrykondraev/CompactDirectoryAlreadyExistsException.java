package ru.mail.polis.dmitrykondraev;

import java.nio.file.Path;

/**
 *
 */
public class CompactDirectoryAlreadyExistsException extends IllegalStateException {
    private final Path directory;

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
