package ru.mail.polis.dmitrykondraev;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BackgroundIOExecutor {
    private final ExecutorService delegate =
            new ThreadPoolExecutor(0, 1,
                    0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

    public void execute(IOCommand command) throws RejectedExecutionException {
        delegate.execute(() -> {
            try {
                command.run();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public boolean awaitTerminationIndefinitely() throws InterruptedException {
        delegate.shutdown();
        return delegate.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    public interface IOCommand {
        void run() throws IOException;
    }
}
