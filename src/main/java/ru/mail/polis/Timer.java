package ru.mail.polis;

public class Timer {

    private long startTime;

    public Timer() {
        this.startTime = System.nanoTime();
    }

    public String elapse() {
        double elapse = System.nanoTime() - startTime;
        long times;
        StringBuilder resultStr = new StringBuilder();

        for (Time time: Time.values()) {
            if (Double.compare(elapse * time.getFactor(), 0) > 0) {
                times = (long)Math.floor(elapse * time.getFactor());
                elapse -= times * time.getFactor();
                resultStr.append(times).append(time.getSystem()).append(":");
            }
        }

        return resultStr.toString();
    }

    public void refresh() {
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
