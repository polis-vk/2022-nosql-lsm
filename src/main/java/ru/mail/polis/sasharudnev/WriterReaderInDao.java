package ru.mail.polis.sasharudnev;


import ru.mail.polis.BaseEntry;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

class WriterReaderInDao {

    static BaseEntry<String> readInDao(Path path, String key) throws IOException {
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

    static void writeInDao(Path path, Map<String, BaseEntry<String>> map) throws IOException {
        try (DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)))) {
            writer.writeInt(map.size());
            for (BaseEntry<String> entry : map.values()) {
                writer.writeUTF(entry.key());
                writer.writeUTF(entry.value());
            }
        }
    }
}
