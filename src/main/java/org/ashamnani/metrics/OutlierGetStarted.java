package org.ashamnani.metrics;

import java.util.Arrays;
import java.util.stream.IntStream;

public class OutlierGetStarted {
    static OutlierDetector detector = new OutlierDetector(40, 0.3, EventRateAlgorithm.EXPONENTIAL_MOVING_AVG_RATE, ReportingChannel.SLF4J, 5);
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
                i++;
                Arrays.asList(new String[]{"289", "3231","3434", "5643"}).forEach(tenant -> {
                    System.out.println(String.format("Tenant %s status -> %s", tenant, detector.isOutlier(tenant)));
                });

            } catch (InterruptedException e) {
            }
        }
    }
}