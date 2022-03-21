package ru.mail.polis;

import javax.annotation.Nullable;

public interface Entry<D> {
    D key();

    @Nullable
    D value();
}
