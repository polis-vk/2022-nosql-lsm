package ru.mail.polis.pavelkovalenko;

import java.time.Clock;

public class Timer {

    private final Clock clock = Clock.systemDefaultZone();
    private long startTime;

    public Timer() {
        this.startTime = clock.millis();
    }

    public double elapse() {
        return (double)(clock.millis() - startTime) / 1000;
    }

    public void refresh() {
        startTime = clock.millis();
    }

}
