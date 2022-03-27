package ru.mail.polis.pavelkovalenko;

public class Timer {

    private long startTime;

    public Timer() {
        this.startTime = System.nanoTime();
    }

    private enum Time {
        SECONDS(0, "s"),
        MILLISECONDS(3, "ms"),
        MICROSECONDS(6, "mcs"),
        NANOSECONDS(9, "ns");

        private final double multiplier;
        private final double divider;
        private final String system;

        Time(double factor, String system) {
            this.multiplier = Math.pow(10, factor);
            this.divider = Math.pow(10, -factor);
            this.system = system;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public double getDivider() {
            return divider;
        }

        public String getSystem() {
            return system;
        }
    }

    public String elapse() {
        double result = (System.nanoTime() - startTime) / Math.pow(10, 9);
        long times;
        StringBuilder resultStr = new StringBuilder();

        for (Time time: Time.values()) {
            if (Double.compare(result * time.getMultiplier(), 0) > 0) {
                times = (long)Math.floor(result * time.getMultiplier());
                result -= times * time.getDivider();
                resultStr.append(times).append(time.getSystem()).append(":");
            }
        }

        return resultStr.toString();
    }

    public void refresh() {
        startTime = System.nanoTime();
    }

}
