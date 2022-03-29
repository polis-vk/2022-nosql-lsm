package ru.mail.polis.kirillpobedonostsev;

import java.nio.ByteBuffer;

public record FilePair(ByteBuffer indexFile, ByteBuffer dataFile) {
}
