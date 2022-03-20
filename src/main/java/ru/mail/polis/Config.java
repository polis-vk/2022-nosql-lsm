package ru.mail.polis;

import edu.umd.cs.findbugs.annotations.NonNull;

import java.nio.file.Path;

public record Config(@NonNull Path basePath) {
}
