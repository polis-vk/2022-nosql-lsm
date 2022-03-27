package ru.mail.polis;

public class Timer {

    private static final char TIME_SEPARATOR = ':';
    private long startTime;

    public Timer() {
    }

    public String elapse() {
        double elapse = System.nanoTime() - startTime;
        long times;
        StringBuilder resultStr = new StringBuilder();

        for (Time time: Time.values()) {
            if (Double.compare(elapse * time.getFactor(), 0) > 0) {
                times = (long)Math.floor(elapse * time.getFactor());
                elapse -= times * time.getFactor();
                resultStr.append(times).append(time.getSystem()).append(TIME_SEPARATOR);
            }
        }

        return resultStr.toString();
    }

    public void set() {
        startTime = System.nanoTime();
    }

    // Relatively to nanoseconds
    private enum Time {
        SECONDS(-9, "s"),
        MILLISECONDS(-6, "ms"),
        MICROSECONDS(-3, "mcs"),
        NANOSECONDS(0, "ns");

        private final double factor;
        private final String system;

        Time(double factor, String system) {
            this.factor = Math.pow(10, factor);
            this.system = system;
        }

        public double getFactor() {
            return factor;
        }

        public String getSystem() {
            return system;
        }
    }

}
