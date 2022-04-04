package ru.mail.polis.pavelkovalenko;

import java.nio.MappedByteBuffer;

public record MappedPairedFiles(MappedByteBuffer dataFile, MappedByteBuffer indexesFile) {
}
