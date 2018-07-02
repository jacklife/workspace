package com.es.service;

import com.navercorp.pinpoint.common.bo.SpanBo;

import java.util.List;

public interface TraceDao {

    SpanBo getSpanBo(String transactionId);

    List<SpanBo> getSpanBos(String url, String traceAgentId, long parentId, int limit);

    String getTransactions(String appName, long parentId, long from, long to);

    void insert(int counter, int num);

    void insertCpuInfo(int counter,int num);
}
