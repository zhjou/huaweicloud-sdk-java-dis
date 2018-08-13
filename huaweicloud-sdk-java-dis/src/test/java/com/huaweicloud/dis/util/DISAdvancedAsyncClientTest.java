package com.huaweicloud.dis.util;

import com.huaweicloud.dis.DISAdvancedAsyncClient;
import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import org.junit.Test;


import java.util.concurrent.CountDownLatch;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by z00382129 on 2018/6/27.
 */
public class DISAdvancedAsyncClientTest
{
    private static final int  NUM = 20;
    private String[] getCursors(DISAdvancedAsyncClient disAdvancedAsyncClient)
    {
        CountDownLatch cd = new CountDownLatch(NUM);
        String[] cursors = new String[NUM];
        for(int i=0;i<NUM;i++)
        {
            GetPartitionCursorRequest getPartitionCursorRequest = new GetPartitionCursorRequest();
            getPartitionCursorRequest.setStartingSequenceNumber(String.valueOf(0));
            getPartitionCursorRequest.setStreamName("perf-test");
            getPartitionCursorRequest.setPartitionId(String.format("shardId-%010d", i));
            final int index = i;
            disAdvancedAsyncClient.getPartitionCursor(getPartitionCursorRequest,
                    new IAsyncHttpCallback<GetPartitionCursorResult>()
                    {
                        @Override
                        public void onComplete(GetPartitionCursorResult data, Exception e)
                        {
                            cd.countDown();
                            if(data != null)
                            {
                                System.out.println("partition " + index + " cursor : " + data.getPartitionCursor());
                                cursors[index] =  data.getPartitionCursor();
                            }
                            else if (e != null)
                            {
                                System.out.println("partition " + index + " " + e.getMessage());
                            }
                        }
                    });
        }
        try
        {
            cd.await();
        }
        catch (InterruptedException e)
        {

        }

        return cursors;
    }
    @Test
    public void test1()
    {

        DISConfig config = new DISConfig();
        //config.setEndpoint("https://10.154.74.187:1445");
        config.setEndpoint("https://dis.cn-east-2.myhuaweicloud.com:20004");
        config.setAK("ODO387IHGUPDQRH2BH6Z");
        config.setSK("2xDa0FHfrzDKEooKogZrcghmdqBiWii5XjLCe3Ce");
        config.setProjectId("1fbd643eeee243f080f79271cb880ffa");
        config.setRegion("cn-east-2");
        DISAdvancedAsyncClient disAdvancedAsyncClient = new DISAdvancedAsyncClient(config);
        long cursorsTime = 0;
        String[] cursors = null;
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger complete = new AtomicInteger();
        AtomicLong delay = new AtomicLong();
        long pre = System.currentTimeMillis();
        while (true)
        {
            if(System.currentTimeMillis() - cursorsTime > 4*60*1000)
            {
                cursors = getCursors(disAdvancedAsyncClient);
                cursorsTime = System.currentTimeMillis();
            }
            if(System.currentTimeMillis() - pre > 1000)
            {
                System.out.println("output rate " + complete.get()/5.0);
                if(complete.get() > 0)
                {
                    System.out.println("delay " + delay.get() / complete.get());
                }
                pre = System.currentTimeMillis();
                complete.set(0);
                delay.set(0L);
            }
            while(inFlight.get() < 2000)
            {
                //System.out.println("current request " + inFlight.get());
                for (int i=0;i<8;i++)
                {
                    GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
                    getRecordsRequest.setLimit(1);
                    getRecordsRequest.setPartitionCursor(cursors[i]);
                    int index = i;
                    inFlight.getAndIncrement();
                    long startTime = System.currentTimeMillis();
                    disAdvancedAsyncClient.getRecords(getRecordsRequest, new IAsyncHttpCallback<GetRecordsResult>()
                    {
                        @Override
                        public void onComplete(GetRecordsResult data, Exception e)
                        {
                            inFlight.getAndDecrement();
                            complete.getAndIncrement();
                            delay.getAndAdd(System.currentTimeMillis() - startTime);
//                            if(data != null)
//                            {
//                                System.out.println("partition " + index + ", data limit: " + data.getRecords().size());
//                            }
//                            else if (e != null)
//                            {
//                                System.out.println("partition " + index + " " + e.getMessage());
//                            }
//                            else
//                            {
//                                System.out.println("partition " + index + " error ");
//                            }
                        }
                    });
                }
            }
        }
    }
}
