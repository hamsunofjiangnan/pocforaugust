package com.pactera.poc;

import com.stumbleupon.async.Callback;

/**
 * @Author: hamsun
 * @Description:
 * @Date: 2019/8/3 23:48
 */
public class ErrBack implements Callback<String, Exception> {
    public String call(final Exception e) throws Exception {
        String message = ">>>>>>>>>>>Failure!>>>>>>>>>>>";
        System.err.println(message + " " + e.getMessage());
        e.printStackTrace();
        return message;
    }
}
