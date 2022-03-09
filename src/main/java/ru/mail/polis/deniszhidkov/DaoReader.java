package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

public class DaoReader {

    private final Path pathToFile;

    public DaoReader(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public BaseEntry<String> findEntryByKey(String key) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(pathToFile.toString()))) {
            String value = null;
            String line = reader.readLine();
            while (line != null) {
                String[] entry = line.split(": ");
                if (key.equals(entry[0])) {
                    value = entry[1];
                    break;
                }
                line = reader.readLine();
            }
            return value == null ? null : new BaseEntry<>(key, value);
        }
    }
}
