package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Entry<D> {
    @NotNull
    D key();

    @Nullable
    D value();
}
