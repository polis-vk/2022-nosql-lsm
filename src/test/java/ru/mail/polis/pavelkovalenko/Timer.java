package ru.mail.polis.pavelkovalenko;

public final class Timer {

    private Timer() {
    }

    public static long elapseMs(Lambda action) throws Exception {
        long currentMs = System.currentTimeMillis();
        action.action();
        return System.currentTimeMillis() - currentMs;
    }
}

interface Lambda {
    void action() throws Exception;
}
