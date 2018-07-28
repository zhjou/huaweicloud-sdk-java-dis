package com.huaweicloud.dis;

import com.huaweicloud.dis.core.DISCredentials;
import com.huaweicloud.dis.core.DefaultRequest;
import com.huaweicloud.dis.core.Request;
import com.huaweicloud.dis.core.http.HttpMethodName;
import com.huaweicloud.dis.core.restresource.CursorResource;
import com.huaweicloud.dis.core.restresource.RecordResource;
import com.huaweicloud.dis.core.restresource.ResourcePathBuilder;
import com.huaweicloud.dis.core.util.StringUtils;
import com.huaweicloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.huaweicloud.dis.iface.data.request.GetRecordsRequest;
import com.huaweicloud.dis.iface.data.response.GetPartitionCursorResult;
import com.huaweicloud.dis.iface.data.response.GetRecordsResult;
import com.huaweicloud.dis.util.IAsyncHttpCallback;
import com.huaweicloud.dis.util.RestAsyncClientWrapper;
import org.apache.http.HttpRequest;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by z00382129 on 2018/6/27.
 */
public class DISAdvancedAsyncClient extends DISClient
{

    public DISAdvancedAsyncClient(DISConfig disConfig)
    {
        super(disConfig);
    }

    public DISAdvancedAsyncClient()
    {
        super();
    }

    private void setEndpoint(Request<HttpRequest> request, String endpointStr)
    {
        URI endpoint;
        try
        {
            endpoint = new URI(endpointStr);
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }

        request.setEndpoint(endpoint);
    }

    public void getPartitionCursor(GetPartitionCursorRequest getPartitionCursorParam, IAsyncHttpCallback<GetPartitionCursorResult> callback)
    {
        innerGetPartitionCursor(getPartitionCursorParam,callback);
    }

    /**
     * Internal API
     */
    protected void innerGetPartitionCursor(GetPartitionCursorRequest getPartitionCursorParam,IAsyncHttpCallback<GetPartitionCursorResult> callback)
    {
        Request<HttpRequest> request = new DefaultRequest<>(Constants.SERVICENAME);
        request.setHttpMethod(HttpMethodName.GET);

        final String resourcePath =
                ResourcePathBuilder.standard()
                        .withProjectId(disConfig.getProjectId())
                        .withResource(new CursorResource(null))
                        .build();
        request.setResourcePath(resourcePath);
        setEndpoint(request, disConfig.getEndpoint());

        request(getPartitionCursorParam, request, GetPartitionCursorResult.class,callback);
    }

    public void getRecords(GetRecordsRequest getRecordsParam,IAsyncHttpCallback<GetRecordsResult> callback)
    {
        innerGetRecords(getRecordsParam,callback);
    }

    /**
     * Internal API
     */
    protected void innerGetRecords(GetRecordsRequest getRecordsParam,IAsyncHttpCallback<GetRecordsResult> callback)
    {
        Request<HttpRequest> request = new DefaultRequest<>(Constants.SERVICENAME);
        request.setHttpMethod(HttpMethodName.GET);

        final String resourcePath =
                ResourcePathBuilder.standard()
                        .withProjectId(disConfig.getProjectId())
                        .withResource(new RecordResource(null))
                        .build();
        request.setResourcePath(resourcePath);
        setEndpoint(request, disConfig.getEndpoint());
        request(getRecordsParam, request, GetRecordsResult.class,callback);
    }


    protected <T> void request(Object param, Request<HttpRequest> request, Class<T> clazz,IAsyncHttpCallback<T> callback)
    {
        DISCredentials credentials = this.credentials;
        if (credentialsProvider != null)
        {
            DISCredentials cloneCredentials = this.credentials.clone();
            credentials = credentialsProvider.updateCredentials(cloneCredentials);
            if (credentials != cloneCredentials)
            {
                this.credentials = credentials;
            }
        }

        request.addHeader(HTTP_X_PROJECT_ID, disConfig.getProjectId());

        String securityToken = credentials.getSecurityToken();
        if (!StringUtils.isNullOrEmpty(securityToken))
        {
            request.addHeader(HTTP_X_SECURITY_TOKEN, securityToken);
        }

        // 发送请求
        new RestAsyncClientWrapper(request, disConfig)
                .request(param, credentials.getAccessKeyId(), credentials.getSecretKey(), region, clazz,callback);
    }
}
