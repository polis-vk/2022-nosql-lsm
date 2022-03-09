package ru.mail.polis;

import java.io.Serializable;

public interface Entry<D> extends Serializable {
    D key();

    D value();
}
