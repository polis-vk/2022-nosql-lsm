package ru.mail.polis.pavelkovalenko.utils;

public final class Timer {

    public static final Timer INSTANSE = new Timer();
    private long startTime;

    private Timer() {
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
            System.out.println();
            return system;
        }
    }

    public void set() {
        startTime = System.nanoTime();
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

}
