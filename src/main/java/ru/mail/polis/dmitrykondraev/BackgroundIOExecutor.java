package ru.mail.polis.dmitrykondraev;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class BackgroundIOExecutor {
    public final ExecutorService service =
            new ThreadPoolExecutor(0, 1,
                    0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

    public void execute(IOCommand command) throws RejectedExecutionException {
        service.execute(IOCommand.unchecked(command));
    }

    public Future<?> submit(IOCommand command) {
        return service.submit(IOCommand.unchecked(command));
    }

    public interface IOCommand {
        void run() throws IOException;

        static Runnable unchecked(IOCommand command) {
            return () -> {
                try {
                    command.run();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        }
    }
}
