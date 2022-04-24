package ru.mail.polis.dmitrykondraev;

import java.nio.file.Path;

/**
 * Tells that previous compact had a failure before renaming compacted directory.
 * Could be used to restore state before compactification.
 */
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
