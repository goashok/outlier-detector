package org.ashamnani.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Created by ashok on 3/25/17.
 */
public class ReportingInterval {
    private long interval;
    private TimeUnit unit;

    /**
     * Pre defined ReportinInterval(s)
     */
    public static final ReportingInterval PER_SEC = new ReportingInterval(1, TimeUnit.SECONDS);
    public static final ReportingInterval PER_5_SEC = new ReportingInterval(5, TimeUnit.SECONDS);
    public static final ReportingInterval PER_10_SEC = new ReportingInterval(10, TimeUnit.SECONDS);
    public static final ReportingInterval PER_30_SEC = new ReportingInterval(30, TimeUnit.SECONDS);
    public static final ReportingInterval PER_MIN = new ReportingInterval(1, TimeUnit.MINUTES);


    public ReportingInterval(long interval, TimeUnit unit) {
        this.interval = interval;
        this.unit = unit;
    }

    public long getInterval() {
        return interval;
    }

    public TimeUnit getUnit() {
        return unit;
    }
}
