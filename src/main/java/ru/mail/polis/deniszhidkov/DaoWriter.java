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

    private final Path pathToDataFile;
    private final Path pathToOffsetsFile;

    public DaoWriter(Path pathToDataFile, Path pathToOffsetsFile) {
        this.pathToDataFile = pathToDataFile;
        this.pathToOffsetsFile = pathToOffsetsFile;
    }

    public void writeDAO(Map<String, BaseEntry<String>> map) throws IOException {
        try (DataOutputStream dataWriter = new DataOutputStream(
                new BufferedOutputStream(
                        Files.newOutputStream(
                                pathToDataFile,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE
                        )));
             DataOutputStream offsetsWriter = new DataOutputStream(
                     new BufferedOutputStream(
                             Files.newOutputStream(
                                     pathToOffsetsFile,
                                     StandardOpenOption.CREATE,
                                     StandardOpenOption.WRITE
                             )))) {
            dataWriter.writeInt(map.size());
            offsetsWriter.writeInt(map.size());
            offsetsWriter.writeLong(dataWriter.size());
            for (BaseEntry<String> entry : map.values()) {
                dataWriter.writeUTF(entry.key());
                if (entry.value() == null) {
                    dataWriter.writeBoolean(false);
                } else {
                    dataWriter.writeBoolean(true);
                    dataWriter.writeUTF(entry.value());
                }
                offsetsWriter.writeLong(dataWriter.size());
            }
        }
    }
}
