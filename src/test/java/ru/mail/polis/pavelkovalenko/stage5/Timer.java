package ru.mail.polis.pavelkovalenko.stage5;

public final class Timer {

    private Timer() {
    }

    public static long elapseMs(Task task) throws Exception {
        long startMs = System.currentTimeMillis();
        task.execute();
        return System.currentTimeMillis() - startMs;
    }
}

interface Task {
    void execute() throws Exception;
}
