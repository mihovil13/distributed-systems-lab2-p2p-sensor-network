package hr.fer.tel.rassus.stupidudp.Reading;

import java.util.HashMap;
import java.util.Map;

public class Reading {
    long scalarClock;
    Map<String, Integer> vectorClock;
    String no2;

    public Reading(long scalarClock, Map<String, Integer> vectorClock, String no2) {
        this.scalarClock = scalarClock;
        this.vectorClock = new HashMap<>(vectorClock);
        this.no2 = no2;
    }

    @Override
    public String toString() {
        return "NO2=" + no2 + " scalar=" + scalarClock + " vector=" + vectorClock;
    }

    public long getScalarClock() {
        return scalarClock;
    }

    public Map<String, Integer> getVectorClock() {
        return vectorClock;
    }

    public String getNo2() {
        return no2;
    }
}
