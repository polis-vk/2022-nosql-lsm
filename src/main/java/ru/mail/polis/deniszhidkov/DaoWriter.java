package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class DaoWriter {

    private final Path pathToFile;

    public DaoWriter(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public void writeDAO(Map<String, BaseEntry<String>> map) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(pathToFile.toString()))) {
            for (Map.Entry<String, BaseEntry<String>> entry : map.entrySet()) {
                String line = entry.getKey() + ": " + entry.getValue().value();
                writer.write(line);
                writer.newLine();
            }
        }
    }
}
