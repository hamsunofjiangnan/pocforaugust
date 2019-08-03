package com.pactera.poc;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.*;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import pojo.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: hamsun
 * @Description:
 * @Date: 2019/8/3 18:55
 */
public class OperationMap extends RichMapFunction<String,Integer> {
    public OperationMap(){}
    Query[] tsdbqueries;
    TSDB tsdb;
    private Gson gson;
    final String metricName = "my.tsdb.test.metric";
    Map<String, String> tags;
    @Override
    public void open(Configuration parameters) throws Exception {
        gson = new GsonBuilder().create();
        java.lang.String pathToConfigFile = parameters.getString("pathToConfigFile", "");
        final Config config;
        if (pathToConfigFile != null && !pathToConfigFile.isEmpty()) {
            config = new Config(pathToConfigFile);
        } else {
            config = new Config(true);
        }
        tsdb = new TSDB(config);
        final TSQuery query = new TSQuery();
        query.setStart("1n-ago");
        final TSSubQuery subQuery = new TSSubQuery();
        subQuery.setMetric(metricName);
        subQuery.setDownsample("all-sum");
        final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
        filters.add(new TagVFilter.Builder()
                .setType("literal_or")
                .setFilter("example2")
                .setTagk("script1")
                .setGroupBy(true)
                .build());
        subQuery.setFilters(filters);

        subQuery.setAggregator("sum");

        final ArrayList<TSSubQuery> subQueries = new ArrayList<TSSubQuery>(1);
        subQueries.add(subQuery);
        query.setQueries(subQueries);
        query.setMsResolution(true);
        query.validateAndSetQuery();
        tsdbqueries = query.buildQueries(tsdb);

        tags = new HashMap<String, String>(1);
        tags.put("script1", "example2");
    }

    @Override
    public Integer map(String value) throws Exception {
        //查询一个时间(一个月)区间的值》》》》》》》》》》》》》》》》》》》》》》》》》》》》
        long startTime = DateTime.nanoTime();
        final int nqueries = tsdbqueries.length;
        ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(
                nqueries);
        ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<Deferred<DataPoints[]>>(nqueries);

        for (int i = 0; i < nqueries; i++) {
            deferreds.add(tsdbqueries[i].runAsync());
        }
        class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
            public Object call(final ArrayList<DataPoints[]> queryResults)
                    throws Exception {
                results.addAll(queryResults);
                return null;
            }
        }

        class QueriesEB implements Callback<Object, Exception> {
            @Override
            public Object call(final Exception e) throws Exception {
                System.err.println("Queries failed");
                e.printStackTrace();
                return null;
            }
        }

        try {
            Deferred.groupInOrder(deferreds)
                    .addCallback(new QueriesCB())
                    .addErrback(new QueriesEB())
                    .join();
        } catch (Exception e) {
            e.printStackTrace();
        }
        double elapsedTime = DateTime.msFromNanoDiff(DateTime.nanoTime(), startTime);
        System.out.println("Query returned in: " + elapsedTime + " milliseconds.");
        for (final DataPoints[] dataSets : results) {
            for (final DataPoints data : dataSets) {
                System.out.print(data.metricName());
                Map<java.lang.String, java.lang.String> resolvedTags = data.getTags();
                for (final Map.Entry<java.lang.String, java.lang.String> pair : resolvedTags.entrySet()) {
                    System.out.print(" " + pair.getKey() + "=" + pair.getValue());
                }
                System.out.print("\n");

                final SeekableView it = data.iterator();
                while (it.hasNext()) {
                    final DataPoint dp = it.next();
                    System.out.println("  " + dp.timestamp() + " "
                            + (dp.isInteger() ? dp.longValue() : dp.doubleValue()));
                }
                System.out.println("");
            }
        }
        //》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》

        //写入opentsdb》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》
        Message message = gson.fromJson(value, Message.class);
        System.out.println("进件单数="+message.getNoip());
        byte[] byteMetricUID;

        try {
            byteMetricUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, metricName);
        } catch (IllegalArgumentException iae) {
            System.out.println("Metric name not valid.");
            iae.printStackTrace();
            System.exit(1);
        } catch (NoSuchUniqueName nsune) {
            byteMetricUID = tsdb.assignUid("metric", metricName);
        }
        long timestamp = System.currentTimeMillis() / 1000;
        long data = message.getNoip();

        long startTime1 = System.currentTimeMillis();
        ArrayList<Deferred<Object>> deferredsAdd = new ArrayList<Deferred<Object>>(1);
        Deferred<Object> deferred = tsdb.addPoint(metricName, timestamp, data, tags);
        deferredsAdd.add(deferred);
        System.out.println("Waiting for deferred result to return...");
        Deferred.groupInOrder(deferreds)
                .addErrback(new ErrBack())
                .addCallback(new SuccBack())
                .join();
        long elapsedTime1 = System.currentTimeMillis() - startTime1;
        System.out.println("\nAdding " + 1 + " points took: " + elapsedTime1
                + " milliseconds.\n");
        class errBack implements Callback<String, Exception> {
            public String call(final Exception e) throws Exception {
                String message = ">>>>>>>>>>>Failure!>>>>>>>>>>>";
                System.err.println(message + " " + e.getMessage());
                e.printStackTrace();
                return message;
            }
        };

        class succBack implements Callback<Object, ArrayList<Object>> {
            public Object call(final ArrayList<Object> results) {
                System.out.println("Successfully wrote " + results.size() + " data points");
                return null;
            }
        };
        //》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》
        return null;
    }

    @Override
    public void close() throws Exception {
        tsdb.shutdown().join();
    }
}
