package ru.mail.polis;

import javax.annotation.Nullable;

public record BaseEntry<Data>(Data key, @Nullable Data value) implements Entry<Data> {
    @Override
    public String toString() {
        return "{" + key + ":" + value + "}";
    }
}
