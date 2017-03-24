package org.ashamnani.metrics;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by ashok on 3/23/17.
 * Outlier Detector detects outlier groups which produce/process event rates greater
 * than <code>outlierMessageRateThreshold</code>. If the percentage of outliers in the group
 * has reached <code>maxOutlierPercent</code>  no further groups are deemed as outliers even though
 * their event rates could be higher than the <code>outlierMessageRateThreshold</code>. This is prevent
 * deeming all groups as outliers and every group moving out of the normal processing paradigm when a alternate processing
 * paradigm is set for the outliers. If you really want to deem all groups as outliers for any reason, and move
 * all of them to an alternate processing paradigm , you can set the <code>maxOutlierPercent</code> to 100%
 */
public class OutlierDetector {

    private Map<String, GroupStats> groupStats = new LinkedHashMap<>();
    private ScheduledReporter reporter;
    private MetricRegistry registry = new MetricRegistry();

    private int outlierMessageRateThreshold;
    private double maxOutlierPercent;
    private EventRateAlgorithm algorithm;
    private int reportingIntervalSecs;
    private Logger LOGGER = LoggerFactory.getLogger(OutlierDetector.class);


    /**
     * Detects outlier groups which produce/process event rates greater
     * than <code>outlierMessageRateThreshold</code>. If the percentage of outliers in the group
     * has reached <code>maxOutlierPercent</code>  no further groups are deemed as outliers even though
     * their event rates could be higher than the <code>outlierMessageRateThreshold</code>. This is prevent
     * deeming all groups as outliers and every group moving out of the normal processing paradigm when a alternate processing
     * paradigm is set for the outliers. If you really want to deem all groups as outliers for any reason, and move
     * all of them to an alternate processing paradigm , you can set the <code>maxOutlierPercent</code> to 100%
     * @param outlierMessageRateThreshold
     * @param maxOutlierPercent
     * @param algorithm
     * @param channel
     * @param reportingIntervalSecs
     */
    public OutlierDetector(int outlierMessageRateThreshold, double maxOutlierPercent, EventRateAlgorithm algorithm, ReportingChannel channel, int reportingIntervalSecs) {
        this.outlierMessageRateThreshold = outlierMessageRateThreshold;
        this.maxOutlierPercent = maxOutlierPercent;
        this.algorithm = algorithm;
        this.reportingIntervalSecs = reportingIntervalSecs;
        switch(channel) {
            case CONSOLE:
                reporter = ConsoleReporter.forRegistry(registry)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build();
                break;
            case SLF4J:
                reporter =  Slf4jReporter.forRegistry(registry)
                    .outputTo(LoggerFactory.getLogger("org.codehale.app.metrics"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
                break;
        }

    }

    public void mark(String groupId) {
        mark(groupId, 1);
    }

    public void mark(String groupId, long n) {
        GroupStats stats = groupStats.get(groupId);
        if (stats == null) {
            synchronized (this) {
                //TODO: Fix DCL . Although in this case its not critical if a new GroupStats get created
                //and replaced with eventually being one GroupStats per groupId.
                if (stats == null) {
                    SlidingTimeWindowReservoir histo = new SlidingTimeWindowReservoir(1, TimeUnit.SECONDS);
                    registry.register("msgRateActual-"+groupId, new Histogram(histo));
                    GroupStats s = new GroupStats(groupId, registry.meter("msgRateEMA-" + groupId), histo);
                    groupStats.put(groupId, s);
                    stats = s;
                }
            }
        }
        stats.mark(n);
    }

    /**
     * Returns true if event rate for the group exceeds the threshold based on the outlier algorithm
     * set in the detector. False otherwise.
     * @param groupId
     * @return true if event rate exceeds threshold false otherwise
     */
    public boolean exceedsThresholdRate(String groupId) {
        int numGroups = groupStats.size();
        if(numGroups == 0) {
            throw new IllegalStateException(String.format("No groups registered. Register the group %s first", groupId));
        }

        double currentRate = getEventRate(groupId);

        LOGGER.info(String.format("groupId =%s , currentRate = %.3f, actualRate = %.3f, emaRate = %.3f ", groupId, currentRate, getActualRate(groupId), getEMARate(groupId)));
        if (currentRate > outlierMessageRateThreshold) {
            return true;
        }else {
            return false;
        }
    }

    /**
     *
     */
    public void init() {
        reporter.start(reportingIntervalSecs, TimeUnit.SECONDS);
    }

    /**
     * If given group's event rate > outlierMessageRateThreshold and we still havent reached the
     * maxOutlierPercent mark we can deem this as an OUTLIER.
     * If we have already reached maxOutlierPercent mark , then even though this group's event rate is higher
     * than outlierMessageRateThreshold, we still deem it as GOOD_CITIZEN.
     * See class level javadoc for the reasoning.
     * @param groupId
     * @return boolean
     */
    public Status isOutlier(String groupId) {
        long currentOutliers = groupStats.values().stream().filter(g -> g.getStatus() == Status.OUTLIER).count();
        double currentOutlierPercent = (double) currentOutliers/groupStats.values().size();
        GroupStats stats = groupStats.get(groupId);
        boolean exceedsThreshold = exceedsThresholdRate(groupId);

        //Reset state to GOOD_CITIZEN if this group was an OUTLIER before and
        //now is a GOOD_CITIZEN based on current event rates (based on algorithm)
        if(!exceedsThreshold) {
            stats.setStatus(Status.GOOD_CITIZEN);
        }

        if(exceedsThreshold && currentOutlierPercent < maxOutlierPercent) {
            stats.setStatus(Status.OUTLIER);
        }

        return stats.getStatus();


    }

    /**
     * Returns event rate in events/sec based on the set algorithm of ACUTAL_RATE or EXPONENTIAL_MOVING_AVG_RATE
     * @param groupId
     * @return
     */
    public double getEventRate(String groupId) {
        GroupStats stats = groupStats.get(groupId);
        if (stats == null) {
            throw new IllegalStateException(String.format("No such group exists for outlier detection %s", groupId));
        }
        switch (algorithm) {
            case ACTUAL_RATE:
                return stats.getHisto().getSnapshot().getValues().length;
            case EXPONENTIAL_MOVING_AVG_RATE:
                return stats.getMeter().getOneMinuteRate();
        }
        throw new IllegalStateException(String.format("Unsupported algorith %s", algorithm));
    }

    /**
     * Returns actual rate , irrespective of what rate algorithm is set
     * @param groupId
     * @return
     */
    public double getActualRate(String groupId) {
        GroupStats stats = groupStats.get(groupId);
        if (stats == null) {
            throw new IllegalStateException(String.format("No such group exists for outlier detection %s", groupId));
        }
        return stats.getHisto().getSnapshot().getValues().length;
    }

    /**
     * Returns EMA rate irrespective of what rate algorithm is set
     * @param groupId
     * @return
     */
    public double getEMARate(String groupId) {
        GroupStats stats = groupStats.get(groupId);
        if (stats == null) {
            throw new IllegalStateException(String.format("No such group exists for outlier detection %s", groupId));
        }
        return stats.getMeter().getOneMinuteRate();
    }

}
