package org.ashamnani.metrics;

/**
 * Created by ashok on 3/24/17.
 */
public enum EventRateAlgorithm {
    /**
     * Using this algorithm, the outliers will be detected based on their
     * actual events in a given second. This can be choppy as event rates for
     * groups can greatly vary within a second and the detector will constantly mark and swap
     * OUTLIER(s) based on their to the second event rate event with the slight variations.
     */
    ACTUAL_RATE_PER_SEC,
    /**
     * Using this algorithm, the outliers will be detected based on their exponential
     * moving average per minute. This will be smoother as their is exponential decay of the
     * monitored event rate , and the detector will not constantly mark and swap OUTLIER(s) with
     * small variations. With bigger variations it still will and that is what is needed.
     */
    EXPONENTIAL_MOVING_AVG_RATE_PER_MIN
}
