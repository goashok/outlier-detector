package org.ashamnani.metrics;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
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

    private Map<String, GroupStats> groupStats = new ConcurrentHashMap<>();
    private ScheduledReporter reporter;
    private MetricRegistry registry = new MetricRegistry();

    private int outlierMessageRateThreshold;
    private double maxOutlierPercent;
    private EventRateAlgorithm algorithm;
    private ReportingInterval reportingInterval;
    private Timer timer = new Timer();
    private Logger LOGGER = LoggerFactory.getLogger(OutlierDetector.class);


    /**
     * Detects outlier groups which produce/process event rates greater
     * than <code>outlierMessageRateThreshold</code>. If the percentage of outliers in the group
     * has reached <code>maxOutlierPercent</code>  no further groups are deemed as outliers even though
     * their event rates could be higher than the <code>outlierMessageRateThreshold</code>. This is prevent
     * deeming all groups as outliers and every group moving out of the normal processing paradigm when a alternate processing
     * paradigm is set for the outliers. If you really want to deem all groups as outliers for any reason, and move
     * all of them to an alternate processing paradigm , you can set the <code>maxOutlierPercent</code> to 100%
     * @param outlierMessageRateThreshold threshold in number of messages per second.
     * @param maxOutlierPercent maximum percentage outliers to deem
     * @param algorithm algorithm to consider for event rate, ACTUAL_RATE_PER_SEC vs EXPONENTIAL_MOVING_AVG_RATE_PER_MIN
     * @param channel channel to report stats on

     */
    public OutlierDetector(int outlierMessageRateThreshold, double maxOutlierPercent, EventRateAlgorithm algorithm, ReportingChannel channel, ReportingInterval reportingInterval) {
        this.outlierMessageRateThreshold = outlierMessageRateThreshold;
        this.maxOutlierPercent = maxOutlierPercent;
        this.algorithm = algorithm;
        this.reportingInterval = reportingInterval;
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

    void mark(String groupId) {
        mark(groupId, 1);
    }

    void mark(String groupId, long n) {
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
     * @param groupId id of the group
     * @return true if event rate exceeds threshold false otherwise
     */
    private boolean exceedsThresholdRate(String groupId) {
        int numGroups = groupStats.size();
        if(numGroups == 0) {
            throw new IllegalStateException(String.format("No groups registered. Register the group %s first", groupId));
        }

        double currentRate = getEventRate(groupId);

        LOGGER.info(String.format("groupId =%s , currentRate = %.3f, actualRate = %.3f, emaRate = %.3f ", groupId, currentRate, getActualRate(groupId), getEMARate(groupId)));

        return currentRate > outlierMessageRateThreshold;
    }

    /**
     *
     */
    public void init() {
        reporter.start(reportingInterval.getInterval(), reportingInterval.getUnit());
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                detectOutliers();
            }
        }, 1000, 1000);
    }

    /**
     * shutsdown the timer.
     */
    public void shutdown() {
        timer.cancel();
        groupStats.clear();
    }

    /**
     * If given group's event rate > outlierMessageRateThreshold and we still havent reached the
     * maxOutlierPercent mark we can deem this as an OUTLIER.
     * If we have already reached maxOutlierPercent mark , then even though this group's event rate is higher
     * than outlierMessageRateThreshold, we still deem it as GOOD_CITIZEN.
     * If a new group
     * See class level javadoc for the reasoning.
     *
     */
    private void detectOutliers() {
        groupStats.values().forEach(g -> {
            String groupId = g.getGroupId();
            long currentOutliers = groupStats.values().stream().filter(gr -> gr.getStatus() == Status.OUTLIER).count();
            double currentOutlierPercent = (double) currentOutliers/groupStats.values().size();
            GroupStats stats = groupStats.get(groupId);
            boolean exceedsThreshold = exceedsThresholdRate(groupId);

            //Reset state to GOOD_CITIZEN if this group was an OUTLIER before and
            //now is a GOOD_CITIZEN based on current event rates (based on algorithm)
            if(!exceedsThreshold) {
                stats.setStatus(Status.GOOD_CITIZEN);
            }

            /**
             * We have  a group that has exceeded the event rate threshold and we still
             * have space for outliers. Make it an outlier.
             */
            if(exceedsThreshold && currentOutlierPercent < maxOutlierPercent) {
                stats.setStatus(Status.OUTLIER);
            }
            /**
             * If this group currently holds the status of GOOD_CITIZEN but  has higher rate of
             * events than threshold, and we already have reached our maxOutlierPercentage ,
             * lets find the current outliers who have lower rate than this group and swap them
             * with if this is a worse outlier than existing ones.
             */
            if(stats.getStatus() == Status.GOOD_CITIZEN && exceedsThreshold && currentOutlierPercent > maxOutlierPercent) {
                Comparator<GroupStats> byEventRate = (GroupStats g1, GroupStats g2) -> getEventRate(g1.getGroupId()) < getEventRate(g2.getGroupId()) ? -1 : 1;
                Optional<GroupStats> gsOp = groupStats.values().stream().filter(x -> x.getStatus() == Status.OUTLIER).sorted(byEventRate).findFirst();
                gsOp.ifPresent(currOutlier -> {
                    if(getEventRate(currOutlier.getGroupId()) < getEventRate(stats.getGroupId())) {
                        LOGGER.info("Making current outlier group {} to GOOD_CITIZEN and making group {} as OUTLIER " +
                                "now as group {} has higher rate than current outlier {}", currOutlier.getGroupId(), stats.getGroupId(), stats.getGroupId(),currOutlier.getGroupId());
                        currOutlier.setStatus(Status.GOOD_CITIZEN);
                        stats.setStatus(Status.OUTLIER);
                    }
                });
            }

        });
    }


    public Status getStatus(String groupId) {
        return Optional.ofNullable(groupStats.get(groupId)).map(g -> g.getStatus()).orElseThrow(() ->new IllegalStateException(String.format("No group with id found %s", groupId)));
    }

    /**
     * Returns event rate in events/sec based on the set algorithm of ACUTAL_RATE or EXPONENTIAL_MOVING_AVG_RATE_PER_MIN
     * @param groupId id of group
     * @return event rate in events per sec
     */
    private double getEventRate(String groupId) {
        GroupStats stats = groupStats.get(groupId);
        if (stats == null) {
            throw new IllegalStateException(String.format("No such group exists for outlier detection %s", groupId));
        }
        switch (algorithm) {
            case ACTUAL_RATE_PER_SEC:
                return stats.getHisto().getSnapshot().getValues().length;
            case EXPONENTIAL_MOVING_AVG_RATE_PER_MIN:
                return stats.getMeter().getOneMinuteRate();
        }
        throw new IllegalStateException(String.format("Unsupported algorith %s", algorithm));
    }

    /**
     * Returns actual rate , irrespective of what rate algorithm is set
     * @param groupId id of the group
     * @return <code>double</code> actual rate of events per sec
     */
    private double getActualRate(String groupId) {
        GroupStats stats = groupStats.get(groupId);
        if (stats == null) {
            throw new IllegalStateException(String.format("No such group exists for outlier detection %s", groupId));
        }
        return stats.getHisto().getSnapshot().getValues().length;
    }

    /**
     * Returns EMA rate irrespective of what rate algorithm is set
     * @param groupId id of the group
     * @return <code>double</code> exponential moving average of events per sec
     */
    private double getEMARate(String groupId) {
        GroupStats stats = groupStats.get(groupId);
        if (stats == null) {
            throw new IllegalStateException(String.format("No such group exists for outlier detection %s", groupId));
        }
        return stats.getMeter().getOneMinuteRate();
    }

}
