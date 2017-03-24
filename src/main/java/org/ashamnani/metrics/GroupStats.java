package org.ashamnani.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SlidingTimeWindowReservoir;

/**
 * Created by ashok on 3/23/17.
 * A Statistics representation of a group as defined by groupId
 */
public class GroupStats {
    private String groupId;
    /**
     * Provides EMA rates
     */
    private Meter meter;
    /**
     * Provides Actual Rates
     */
    private SlidingTimeWindowReservoir histo;
    /**
     * Current status of the Group whether is currently an OUTLIER OR GOOD_CITIZEN
     */
    private Status status = Status.GOOD_CITIZEN;


    public GroupStats(String groupId, Meter meter, SlidingTimeWindowReservoir histo) {
        this.groupId = groupId;
        this.meter = meter;
        this.histo = histo;
    }

    /**
     * Returns groupId associated with this statistics
     * @return
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Returns the meter which captures the metering statistics
     * @return <code>{@link Meter}</code>
     */
    Meter getMeter() {
        return meter;
    }

    /**
     * Sets the status of this group
     * @param status
     */
    void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Returns status of this group
     * @return <code>{@link Status}</code>
     */
    Status getStatus() {
        return status;
    }

    /**
     * Mark the occurrence of a given number of events.
     * @param n number of events
     */
    void mark(long n) {
        meter.mark(n);
        histo.update(n);
    }

    /**
     * Returns the SlidingTimeWindowReservoid (a histogram) that track histogram statistics for this
     * group
     * @return <code>{@link SlidingTimeWindowReservoir}</code>
     */
    SlidingTimeWindowReservoir getHisto() {
        return histo;
    }
}
