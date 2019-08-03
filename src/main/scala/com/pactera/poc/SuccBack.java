package com.pactera.poc;

import com.stumbleupon.async.Callback;
import net.opentsdb.core.DataPoints;

import java.util.ArrayList;

/**
 * @Author: hamsun
 * @Description:
 * @Date: 2019/8/3 23:48
 */
public class SuccBack implements Callback<Object, ArrayList<DataPoints[]>> {
    public Object call(final ArrayList<DataPoints[]> results) {
        System.out.println("Successfully wrote " + results.size() + " data points");
        return null;
    }
}
