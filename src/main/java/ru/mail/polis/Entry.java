package ru.mail.polis;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface Entry<D> {
    D key();

    @Nullable
    D value();
}
