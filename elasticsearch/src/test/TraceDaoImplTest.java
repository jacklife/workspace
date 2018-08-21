package com.es.service;

import com.navercorp.pinpoint.common.bo.SpanBo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.concurrent.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext.xml"})
public class TraceDaoImplTest {
    public static final Logger logger = LoggerFactory.getLogger(TraceDaoImplTest.class);

    private CountDownLatch count = new CountDownLatch(3);

    private ExecutorService worker = new ThreadPoolExecutor(3, 3, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    @Autowired
    private TraceDao traceDao;

    @Test
    public void test_search_one_span_from_es_by_transactionId() {

        traceDao.getSpanBo("topo^1526230754630^88851977");

    }

    @Test
    public void test_search_five_hundred_spans_from_es_by_url_and_rootSpan_and_rootAgentId() {
        String url = "/serviceTraceDetail.pinpoint";
        String traceAgentId = "bservicea";


        List<SpanBo> spanBos = traceDao.getSpanBos(url, traceAgentId, -1, 500);

        System.out.println(spanBos.size());
    }

    @Test
    public void test_aggs_trace_by_url() {

        String appName = "ranoss1";
        long parentId = -1;

        String aggs = traceDao.getTransactions(appName, parentId, 100000000, 200000000);

        System.out.println(aggs);
    }


    @Test
    public void test_insert_spans_to_seven_index() {

        int counter = 24 * 60 * 60 * 1000;

        for (int num = 1; num <= 7; num++) {
            traceDao.insert(counter, num);
        }
    }

    @Test
    public void test_concurrency_query() {
        String url = "/serviceTraceDetail.pinpoint";
        String traceAgentId = "bservicea";
        String appName = "ranoss1";
        long before = System.currentTimeMillis();
        worker.submit(() -> {
            traceDao.getSpanBo("topo^1526230754630^88851977");
            count.countDown();
        });
        worker.submit(() -> {
            traceDao.getSpanBos(url, traceAgentId, -1, 500);
            count.countDown();
        });
        worker.submit(() -> {


            traceDao.getTransactions(appName, -1, 100000000, 200000000);
            count.countDown();
        });

        try {
            count.await();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long after = System.currentTimeMillis();

        System.out.println(after - before);

    }

    @Test
    public void test_insert_cpuInfo_to_es() {
        ExecutorService worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        CountDownLatch latch = new CountDownLatch(1);
        int counter = 300000000;

        worker.submit(() -> {
            for (int num = 2; num <= 4; num++) {
                traceDao.insertCpuInfo(counter, num);
            }

            latch.countDown();
        });

        try {
            latch.await();
            traceDao.insert(22000000, 1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }


    }

    @Test
    public void test_insert_span_to_es() {
        traceDao.insert(50000000, 1);
    }


}
