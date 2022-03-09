package ru.mail.polis.deniszhidkov;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

import ru.mail.polis.BaseEntry;

public class DaoReader {

    private final Path pathToFile;

    public DaoReader(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public BaseEntry<String> findEntryByKey(String key) throws IOException {
        try (DataInputStream reader = new DataInputStream(new FileInputStream(pathToFile.toString()))) {
            String value = null;
            while (reader.available() != 0) {
                String[] entry = reader.readUTF().split(": ");
                if (key.equals(entry[0])) {
                    value = entry[1];
                    break;
                }
            }
            return value == null ? null : new BaseEntry<>(key, value);
        }
    }
}
