package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record BaseEntry<Data>(@NotNull Data key, @Nullable Data value) implements Entry<Data> {
    @Override
    public String toString() {
        return "{" + key + ":" + value + "}";
    }
}
