package ru.mail.polis.test;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.BaseEntry;
import ru.mail.polis.Config;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.io.IOException;
import java.util.Iterator;

class TestDao<D, E extends Entry<D>> implements Dao<String, Entry<String>> {

    Dao<D, E> delegate;

    final DaoFactory.Factory<D, E> factory;
    final Config config;
    final String name;

    TestDao(DaoFactory.Factory<D, E> factory, Config config) throws IOException {
        this.factory = factory;
        this.config = config;
        this.delegate = factory.createDao(config);

        Class<?> delegateClass = delegate.getClass();
        String packageName = delegateClass.getPackageName();
        String lastPackagePart = packageName.substring(packageName.lastIndexOf('.') + 1);

        this.name = "TestDao<" + lastPackagePart + "." + delegateClass.getSimpleName() + ">";
    }

    public Dao<String, Entry<String>> reopen() throws IOException {
        return new TestDao<>(factory, config);
    }

    @Override
    public Entry<String> get(@NotNull String key) throws IOException {
        E result = delegate.get(factory.fromString(key));
        if (result == null) {
            return null;
        }
        return new BaseEntry<>(
                factory.toString(result.key()),
                factory.toString(result.value())
        );
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) throws IOException {
        Iterator<E> iterator = delegate.get(
                factory.fromString(from),
                factory.fromString(to)
        );
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<String> next() {
                E next = iterator.next();
                String key = factory.toString(next.key());
                String value = factory.toString(next.value());
                return new BaseEntry<>(key, value);
            }
        };
    }

    @Override
    public void upsert(Entry<String> entry) {
        BaseEntry<D> e = new BaseEntry<>(
                factory.fromString(entry.key()),
                factory.fromString(entry.value())
        );
        delegate.upsert(factory.fromBaseEntry(e));
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
