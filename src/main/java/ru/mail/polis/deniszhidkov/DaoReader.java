package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DaoReader {

    private final Path pathToFile;

    public DaoReader(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public BaseEntry<String> findEntryByKey(String key) throws IOException {
        try (DataInputStream reader =
                     new DataInputStream(new BufferedInputStream(Files.newInputStream(
                             pathToFile,
                             StandardOpenOption.READ
                     )))) {
            int size = reader.readInt();
            String res = null;
            for (int i = 0; i < size; i++) {
                String keyOfEntry = reader.readUTF();
                String valueOfEntry = reader.readUTF();
                if (key.equals(keyOfEntry)) {
                    res = valueOfEntry;
                    break;
                }
            }
            return res == null ? null : new BaseEntry<>(key, res);
        }
    }
}
