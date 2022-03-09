package ru.mail.polis.deniszhidkov;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import ru.mail.polis.BaseEntry;

public class DaoWriter {

    private final Path pathToFile;

    public DaoWriter(Path pathToFile) {
        this.pathToFile = pathToFile;
    }

    public void writeDAO(Map<String, BaseEntry<String>> map) throws IOException {
        DataOutputStream writer = new DataOutputStream(new FileOutputStream(pathToFile.toString()));
        for (Map.Entry<String, BaseEntry<String>> entry : map.entrySet()) {
            StringBuilder sb = new StringBuilder(entry.getKey());
            sb.append(": ");
            sb.append(entry.getValue().value());
            writer.writeUTF(sb.toString());
        }
        writer.close();
    }
}
