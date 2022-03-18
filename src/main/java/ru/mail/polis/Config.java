package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;

public record Config(@NotNull Path basePath) {
}
