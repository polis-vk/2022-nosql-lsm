package ru.mail.polis.pavelkovalenko.stage5;

public final class Timer {

    private Timer() {
    }

    public static long elapseMs(Task task) throws Exception {
        long currentMs = System.currentTimeMillis();
        task.execute();
        return System.currentTimeMillis() - currentMs;
    }
}

interface Task {
    void execute() throws Exception;
}
