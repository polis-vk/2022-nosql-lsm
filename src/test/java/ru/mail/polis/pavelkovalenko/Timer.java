package ru.mail.polis.pavelkovalenko;

public final class Timer {

    public static final Timer INSTANSE = new Timer();
    private long startTime;

    private Timer() {
    }

    public long elapse() {
        return System.currentTimeMillis() - startTime;
    }

    public void set() {
        startTime = System.currentTimeMillis();
    }

}
