package ru.mail.polis.kirillpobedonostsev;

import java.nio.MappedByteBuffer;

public record FilePair(MappedByteBuffer indexFile, MappedByteBuffer dataFile) {
}
