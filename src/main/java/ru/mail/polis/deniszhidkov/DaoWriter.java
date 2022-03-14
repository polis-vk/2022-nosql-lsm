package ru.mail.polis.deniszhidkov;

import ru.mail.polis.BaseEntry;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class DaoWriter {

    private final Path pathToFile;

    public DaoWriter(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public void writeDAO(Map<String, BaseEntry<String>> map) throws IOException {
        try (DataOutputStream writer =
                     new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                             pathToFile,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING,
                             StandardOpenOption.WRITE
                     )))) {
            writer.writeInt(map.size());
            for (BaseEntry<String> entry : map.values()) {
                writer.writeUTF(entry.key());
                writer.writeUTF(entry.value());
            }
        }
    }
}
