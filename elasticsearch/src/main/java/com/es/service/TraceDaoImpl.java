package com.es.service;

import com.es.domain.ESConst;
import com.es.utils.EsTestUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.es.domain.CpuInfo;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


@Service
public class TraceDaoImpl implements TraceDao {

    public static final Logger logger = LoggerFactory.getLogger(TraceDaoImpl.class);

    @Autowired
    private Client client;

    @Autowired
    private BulkProcessor bulkProcessor;

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public SpanBo getSpanBo(String transactionId) {
        StopWatch stopWatch = new StopWatch("query span by transactionId");

        stopWatch.start("build dsl");

        List<SpanBo> spanBos = new ArrayList<>();

        SearchRequestBuilder request = client.prepareSearch(ESConst.SPAN_INDEX)
                .setTypes(ESConst.SPAN_TYPE)
                .setSearchType(SearchType.QUERY_THEN_FETCH);

        request.setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery(ESConst.TRANSACTIONID, transactionId)));

        stopWatch.stop();

        stopWatch.start("elastic request");
        SearchResponse response = request.execute().actionGet();

        stopWatch.stop();

        stopWatch.start("deal with result");
        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {
            SpanBo spanBo = new Gson().fromJson(hit.getSourceAsString(), SpanBo.class);
            if (spanBo != null) {

                spanBos.add(spanBo);
            }
        }
        stopWatch.stop();

        logger.info(stopWatch.prettyPrint());

        return spanBos.get(0);
    }

    @Override
    public List<SpanBo> getSpanBos(String url, String traceAgentId, long parentId, int limit) {
        StopWatch stopWatch = new StopWatch("get 500 spans form es ");

        stopWatch.start("build dsl");
        List<SpanBo> spanBos = new ArrayList<>();

        SearchRequestBuilder request = client.prepareSearch(ESConst.SPAN_INDEX)
                .setTypes(ESConst.SPAN_TYPE)
                .setSearchType(SearchType.QUERY_THEN_FETCH);

        request.setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery(ESConst.URL, url))
                .must(QueryBuilders.matchPhraseQuery(ESConst.TIER, traceAgentId))
                .must(QueryBuilders.matchPhraseQuery(ESConst.PARENT_ID, parentId)))
                .setSize(limit);
        stopWatch.stop();

        stopWatch.start("elastic request");
        SearchResponse response = request.execute().actionGet();
        stopWatch.stop();

        stopWatch.start("deal with result");
        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {
            SpanBo spanBo = new Gson().fromJson(hit.getSourceAsString(), SpanBo.class);

            if (spanBo != null) {
                spanBos.add(spanBo);
            }
        }
        stopWatch.stop();
        logger.info(stopWatch.prettyPrint());

        return spanBos;
    }

    @Override
    public String getTransactions(String appName, long parentId, long from, long to) {

        StopWatch stopWatch = new StopWatch("aggs by url");

        stopWatch.start("build dsl");

        TermsAggregationBuilder aggregationBuilders2 = AggregationBuilders.terms("url").field("rpc.keyword")
                .subAggregation(AggregationBuilders.terms("tier").field("traceAgentId.keyword")
                        .subAggregation(AggregationBuilders.sum("errors").field("errCode"))
                        .subAggregation(AggregationBuilders.avg("avg_elapsed").field("elapsed"))
                        .subAggregation(AggregationBuilders.max("max_elapsed").field("elapsed")));


        SearchRequestBuilder request = client.prepareSearch(ESConst.SPAN_INDEX)
                .setTypes(ESConst.SPAN_TYPE)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchPhraseQuery(ESConst.APP_NAME, appName))
                        .must(QueryBuilders.matchPhraseQuery(ESConst.PARENT_ID, parentId))
                        .must(QueryBuilders.rangeQuery(ESConst.SEQUENCE).from(from).to(to)))
                .addAggregation(aggregationBuilders2);
        stopWatch.stop();

        stopWatch.start("elastic request");
        SearchResponse response = request.execute().actionGet();

        Aggregations aggs = response.getAggregations();


        stopWatch.stop();

        logger.info(stopWatch.prettyPrint());

        return aggs.toString();
    }

    @Override
    public void insert(int counter, int num) {
//        int count = counter * (num - 1);
        int count = 22000000;
        System.out.println(EsTestUtils.START_TIME + count);

        StopWatch stopWatch = new StopWatch("SpanInsert");

        stopWatch.start("Insert spans to index" + num);

        while (count < counter) {

            for (int i = 0; i < 2000; i++) {
                SpanBo spanBo = EsTestUtils.buildSpanBos2(count);

                try {
                    bulkProcessor.add(new IndexRequest("span" + num, "span", UUID.randomUUID().toString()).source(mapper.writeValueAsString(spanBo)));
                    count++;
                } catch (JsonProcessingException e) {
                    logger.error(e.getMessage());
                }
            }

            System.out.println(count);

        }

        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());

    }

    @Override
    public void insertCpuInfo(int counter, int num) {
        int count = counter * (num - 1);
        StopWatch stopWatch = new StopWatch("CpuInfo insert test");
        stopWatch.start("Insert cpuInfo to es");

        while (count < counter * num) {
            CpuInfo cpuInfo = EsTestUtils.buildCpuInfo();
            try {
                bulkProcessor.add(new IndexRequest("cpu_test" + num, "cpu_info", UUID.randomUUID().toString()).source(mapper.writeValueAsString(cpuInfo)));
                count++;
            } catch (JsonProcessingException e) {
                logger.error(e.getMessage());
            }

        }
        stopWatch.stop();
        System.out.println(stopWatch.prettyPrint());
    }

}
