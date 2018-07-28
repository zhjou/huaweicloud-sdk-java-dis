package com.huaweicloud.dis.util;


import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.core.Request;
import com.huaweicloud.dis.core.auth.signer.internal.SignerConstants;
import com.huaweicloud.dis.core.http.HttpMethodName;
import com.huaweicloud.dis.http.exception.RestClientResponseException;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by z00382129 on 2018/6/27.
 */
public class RestAsyncClientWrapper
{
    private static final Logger LOG = LoggerFactory.getLogger(RestAsyncClientWrapper.class);

    private static final String HEADER_SDK_VERSION = "X-SDK-Version";

    private DISConfig disConfig;

    private RestAsyncClient restAsyncClient;

    private Request<HttpRequest> request;

    public RestAsyncClientWrapper(Request<HttpRequest> request, DISConfig disConfig)
    {
        this.request = request;
        this.disConfig = disConfig;
        this.restAsyncClient = RestAsyncClient.getInstance(disConfig);
    }

    public <T> void request(Object requestContent, String ak, String sk, String region, Class<T> returnType,IAsyncHttpCallback<T> callback)
    {
        beforeRequest(requestContent, region);

        doRequest(request, requestContent, ak, sk, region, returnType, callback);
    }

    private void beforeRequest(Object requestContent, String region)
    {
        // set request header
        setContentType();
        setSdkVersion();

        // set request parameters
        setParameters(requestContent);

        // set request content
        setContent(requestContent);
    }

    private void setContent(Object requestContent)
    {
        HttpMethodName methodName = request.getHttpMethod();
        if (methodName.equals(HttpMethodName.POST) || methodName.equals(HttpMethodName.PUT))
        {
            if (requestContent instanceof byte[])
            {
                request.setContent(new ByteArrayInputStream((byte[])requestContent));
            }
            else if (requestContent instanceof String || requestContent instanceof Integer)
            {
                request.setContent(new ByteArrayInputStream(Utils.encodingBytes(requestContent.toString())));
            }
            else
            {
                String reqJson = JsonUtils.objToJson(requestContent);
                request.setContent(new ByteArrayInputStream(Utils.encodingBytes(reqJson)));
            }
        }
    }

    private void setContentType()
    {
        // 默认为json格式
        if (!request.getHeaders().containsKey("Content-Type"))
        {
            request.addHeader("Content-Type", "application/json; charset=utf-8");
        }

        if (!request.getHeaders().containsKey("accept"))
        {
            request.addHeader("accept", "*/*; charset=utf-8");
        }
    }

    private void setSdkVersion()
    {
        request.addHeader(HEADER_SDK_VERSION, VersionUtils.getVersion() + "/" + VersionUtils.getPlatform());
    }

    @SuppressWarnings("unchecked")
    private void setParameters(Object requestContent)
    {
        if (request.getHttpMethod().equals(HttpMethodName.GET))
        {
            if (requestContent != null)
            {
                Map<String, String> parametersMap = new HashMap<String, String>();
                Map<String, Object> getParamsObj = null;
                if (requestContent instanceof Map)
                {
                    getParamsObj = (Map<String, Object>)requestContent;
                }
                else
                {
                    String tmpJson = JsonUtils.objToJson(requestContent);
                    getParamsObj = JsonUtils.jsonToObj(tmpJson, HashMap.class);
                }

                if (getParamsObj.size() != 0)
                {
                    for (Map.Entry<String, Object> temp : getParamsObj.entrySet())
                    {
                        Object value = temp.getValue();
                        if (value == null)
                        {
                            continue;
                        }

                        parametersMap.put(temp.getKey(), String.valueOf(value));
                    }
                }

                if (null != parametersMap && parametersMap.size() > 0)
                {
                    request.setParameters(parametersMap);
                }
            }
        }

    }

    // 将Request转为restTemplate的请求参数.由于请求需要签名，故请求的body直接传byte[]，响应的反序列化，可以直接利用spring的messageConvert机制
    private <T> void doRequest(Request<HttpRequest> request, Object requestContent, String ak, String sk, String region,
                               Class<T> returnType,IAsyncHttpCallback<T> callback)
    {

        Map<String, String> parameters = request.getParameters();

        StringBuilder uri = new StringBuilder(request.getEndpoint().toString()).append(request.getResourcePath());

        // Set<String> paramKeys = getParams.keySet();
        if (parameters != null && !parameters.isEmpty())
        {
            uri.append("?");
            for (Map.Entry<String, String> temp : parameters.entrySet())
            {
                uri.append(temp.getKey());
                uri.append("=");
                uri.append(temp.getValue());
                uri.append("&");
            }
        }
        request.getHeaders().remove(SignerConstants.AUTHORIZATION);
        request = SignUtil.sign(request, ak, sk, region);
        restAsyncClient.exchange(uri
                .toString(), request.getHttpMethod(), request.getHeaders(), requestContent, returnType,callback);
    }

    public static byte[] toByteArray(InputStream input)
            throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n = 0;
        while (-1 != (n = input.read(buffer)))
        {
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }


    /**
     * 判断此异常是否可以重试
     *
     * @param t
     * @return
     */
    protected boolean isRetriableSendException(Throwable t)
    {
        // 对于连接超时/网络闪断/Socket异常/服务端5xx错误进行重试
        return t instanceof ConnectTimeoutException || t instanceof NoHttpResponseException
                || t instanceof SocketException || t instanceof SSLException
                || (t instanceof RestClientResponseException
                && ((RestClientResponseException)t).getRawStatusCode() / 100 == 5)
                || (t.getCause() != null && isRetriableSendException(t.getCause()));
    }
}
