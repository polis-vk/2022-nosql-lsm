package ru.mail.polis.sasharudnev;

import ru.mail.polis.BaseEntry;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class ReaderInDao {

    private final Path path;

    ReaderInDao (Path path) {
        this.path = path;
    }

    public BaseEntry<String> readInDao(String key) throws IOException {
        try (DataInputStream reader = new DataInputStream(new BufferedInputStream(Files.newInputStream(
                path,
                StandardOpenOption.READ
        )))) {
            int size = reader.readInt();
            for (int i = 0; i < size; ++i) {
                String keyInDaoEntry = reader.readUTF();
                String valueInDaoEntry = reader.readUTF();
                if (key.equals(keyInDaoEntry)) {
                    return new BaseEntry<>(keyInDaoEntry, valueInDaoEntry);
                }
            }
            return null;
        }
    }
}
