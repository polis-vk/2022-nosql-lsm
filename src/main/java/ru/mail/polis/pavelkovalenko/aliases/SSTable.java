package ru.mail.polis.pavelkovalenko.aliases;

import ru.mail.polis.Entry;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable
        extends ConcurrentSkipListMap<ByteBuffer, Entry<ByteBuffer>> {
}
