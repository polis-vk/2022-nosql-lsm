package ru.mail.polis.pavelkovalenko.dto;

import java.nio.file.Path;

public record PairedFiles(Path dataFile, Path indexesFile) {
}
