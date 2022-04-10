package ru.mail.polis.pavelkovalenko.dto;

import java.nio.MappedByteBuffer;

public record MappedPairedFiles(MappedByteBuffer dataFile, MappedByteBuffer indexesFile) {
}
