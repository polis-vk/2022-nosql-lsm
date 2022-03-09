package ru.mail.polis.deniszhidkov;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.nio.file.Path;

import ru.mail.polis.BaseEntry;

public class DaoWriter {

    private final Path pathToFile;

    public DaoWriter(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public void writeDAO(Map<String, BaseEntry<String>> map) throws IOException {
        try (DataOutputStream writer = new DataOutputStream(new FileOutputStream(pathToFile.toString()))) {
            for (Map.Entry<String, BaseEntry<String>> entry : map.entrySet()) {
                String line = entry.getKey() + ": " + entry.getValue().value();
                writer.writeUTF(line);
            }
        }
    }
}
