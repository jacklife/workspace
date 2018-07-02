package com.es.utils;

import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.bo.SpanEventBo;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.es.domain.CpuInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EsTestUtils {

    private static final Logger logger = LoggerFactory.getLogger(EsTestUtils.class);

    public static final String QUERY_BY_TRACE = "{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": [\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"transactionId.keyword\": \"%s\"\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"must_not\": [],\n" +
            "      \"should\": []\n" +
            "    }\n" +
            "  },\n" +
            "  \"from\": 0,\n" +
            "  \"size\": 10,\n" +
            "  \"sort\": [],\n" +
            "  \"aggs\": {}\n" +
            "}";

    public static long START_TIME = 1526230754630L;


    public static SpanBo buildSpanBos2(int sequence) {

        long spanId = 10;

        List<String> urlList = new ArrayList<>();
        urlList.add("/testHello");
        urlList.add("/testGetResult");
        urlList.add("/hosts/dashboard");
        urlList.add("/serviceTraceDetail.pinpoint");

        List<String> agentIdList = new ArrayList<>();
        agentIdList.add("ssm");
        agentIdList.add("log");
        agentIdList.add("test");
        agentIdList.add("topo");

        String agentId = agentIdList.get(new Random().nextInt(4));


        SpanEventBo spanEventBo1 = new SpanEventBo();
        spanEventBo1.setAgentId(agentId);
        spanEventBo1.setSpanId(spanId);
        spanEventBo1.setAgentStartTime(START_TIME);
        spanEventBo1.setDepth(1);
        spanEventBo1.setStartElapsed(new Random().nextInt(100));
        spanEventBo1.setEndElapsed(new Random().nextInt(200));
        spanEventBo1.setRpc("/get");
        spanEventBo1.setApiId(-3);
        spanEventBo1.setNextSpanId(-1);

        SpanEventBo spanEventBo2 = new SpanEventBo();
        spanEventBo2.setAgentId(agentId);
        spanEventBo2.setSpanId(spanId);
        spanEventBo2.setAgentStartTime(START_TIME);
        spanEventBo2.setDepth(1);
        spanEventBo2.setStartElapsed(new Random().nextInt(100));
        spanEventBo2.setEndElapsed(220);
        spanEventBo2.setRpc("/get");
        spanEventBo2.setApiId(-3);
        spanEventBo2.setNextSpanId(-1);


        SpanBo spanBo = new SpanBo();
        spanBo.setTraceAgentId(agentId);
        spanBo.setTraceTransactionSequence(sequence);
        spanBo.setTraceAgentStartTime(START_TIME);
        spanBo.setApplicationId("ranoss_servicea");
        spanBo.setRpc(urlList.get(new Random().nextInt(4)));
        spanBo.setApiId(-19);
        spanBo.setServiceType(ServiceType.JAVA.getCode());

        spanBo.setParentSpanId(-1L);
        spanBo.setSpanID(spanId);
        spanBo.setAgentId(agentId);
        spanBo.setAgentStartTime(START_TIME);
        spanBo.setElapsed(new Random().nextInt(2000));
        spanBo.setErrCode(0);
        spanBo.setStartTime(START_TIME + sequence);
        spanBo.setRemoteAddr("192.168.1.1");


        spanBo.addSpanEvent(spanEventBo1);

        spanBo.addSpanEvent(spanEventBo2);

        return spanBo;

    }

    public static CpuInfo buildCpuInfo() {
        List<String> cpuIdList = new ArrayList<>();
        cpuIdList.add("cpu_0");
        cpuIdList.add("cpu_1");
        cpuIdList.add("cpu_2");
        cpuIdList.add("cpu_3");
        cpuIdList.add("cpu_4");
        cpuIdList.add("cpu_5");
        cpuIdList.add("cpu_6");
        cpuIdList.add("cpu_7");

        CpuInfo cpuInfo = new CpuInfo(1, 2, 3, 4, 5, 6, 7);
        cpuInfo.setMac("fa:16:3e:8d:8c:50");
        cpuInfo.setAgentId("agent_" + new Random().nextInt(1000));
        cpuInfo.setAgentStartTime(1525417726065L);
        cpuInfo.setCollectTime(1525557526065L);
        cpuInfo.setCpuId(cpuIdList.get(new Random().nextInt(8)));

        return cpuInfo;

    }


}
