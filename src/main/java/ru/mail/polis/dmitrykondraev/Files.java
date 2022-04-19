package ru.mail.polis.dmitrykondraev;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

// package-private
class Files {
    static Path createFileIfNotExists(Path path) throws IOException {
        try {
            return java.nio.file.Files.createFile(path);
        } catch (FileAlreadyExistsException ignored) {
            return path;
        }
    }

    static String filenameOf(Path path) {
        return path.getFileName().toString();
    }
}
