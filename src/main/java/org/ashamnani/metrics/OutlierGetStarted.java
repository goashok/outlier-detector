package org.ashamnani.metrics;

import java.util.Arrays;
import java.util.stream.IntStream;

public class OutlierGetStarted {
    static OutlierDetector detector = new OutlierDetector(40, 0.3, EventRateAlgorithm.EXPONENTIAL_MOVING_AVG_RATE_PER_MIN, ReportingChannel.SLF4J, ReportingInterval.PER_MIN);
    public static void main(String args[]) {
        detector.init();
        play();
    }



    static void play() {
        int i = 0;
        while(true) {
            try {
                Thread.sleep(1 * 1000);
                if(i < 5) {
                    IntStream.range(1, 100).forEach(r -> detector.mark("289"));
                    IntStream.range(1, 50).forEach(r -> detector.mark("3434"));
                    IntStream.range(1, 65).forEach(r -> detector.mark("3231"));
                    detector.mark("5643");
                }
                //At some point 3434 will have higher rate than 3434 and it should get swapped.
                if(i > 5) {
                    IntStream.range(1, 5000).forEach(r -> detector.mark("3434"));
                }
                i++;
                Arrays.asList(new String[]{"289", "3231","3434", "5643"}).forEach(tenant -> {
                    System.out.println(String.format("Tenant %s status -> %s", tenant, detector.getStatus(tenant)));
                });

            } catch (InterruptedException e) {
            }
        }
    }
}