package com.huaweicloud.dis.util;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.core.http.HttpMethodName;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.http.DefaultResponseErrorHandler;
import com.huaweicloud.dis.http.HttpMessageConverterExtractor;
import com.huaweicloud.dis.http.ResponseErrorHandler;
import com.huaweicloud.dis.http.ResponseExtractor;
import com.huaweicloud.dis.http.converter.ByteArrayHttpMessageConverter;
import com.huaweicloud.dis.http.converter.HttpMessageConverter;
import com.huaweicloud.dis.http.converter.StringHttpMessageConverter;
import com.huaweicloud.dis.http.converter.json.JsonHttpMessageConverter;
import com.huaweicloud.dis.http.converter.protobuf.ProtobufHttpMessageConverter;
import com.huaweicloud.dis.http.exception.ResourceAccessException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by z00382129 on 2018/6/27.
 */
public class RestAsyncClient
{
    private final Logger logger = LoggerFactory.getLogger(RestAsyncClient.class);

    private final List<HttpMessageConverter<?>> messageConverters = new ArrayList<HttpMessageConverter<?>>();

    private ResponseErrorHandler errorHandler = new DefaultResponseErrorHandler();

    private static RestAsyncClient restAsyncClient;

    private static CloseableHttpAsyncClient httpAsyncClient;

    private DISConfig disConfig;

    private RestAsyncClient(DISConfig disConfig)
    {
        this.disConfig = disConfig;

        this.messageConverters.add(new JsonHttpMessageConverter());
        this.messageConverters.add(new ProtobufHttpMessageConverter());
        this.messageConverters.add(new StringHttpMessageConverter());
        this.messageConverters.add(new ByteArrayHttpMessageConverter());
    }

    private void init()
    {
        httpAsyncClient = getHttpClient();
        httpAsyncClient.start();
    }

    public synchronized static RestAsyncClient getInstance(DISConfig disConfig)
    {
        if (restAsyncClient == null)
        {
            restAsyncClient = new RestAsyncClient(disConfig);
            restAsyncClient.init();
        }

        return restAsyncClient;
    }

    /**
     * Set the message body converters to use.
     * <p>
     * These converters are used to convert from and to HTTP requests and responses.
     */
    public void setMessageConverters(List<HttpMessageConverter<?>> messageConverters)
    {
        if (messageConverters != null && messageConverters.size() > 0)
        {
            // Take getMessageConverters() List as-is when passed in here
            if (this.messageConverters != messageConverters)
            {
                this.messageConverters.clear();
                this.messageConverters.addAll(messageConverters);
            }
        }
    }

    /**
     * Return the message body converters.
     */
    public List<HttpMessageConverter<?>> getMessageConverters()
    {
        return this.messageConverters;
    }

    /**
     * Set the error handler.
     * <p>
     * By default, RestTemplate uses a {@link DefaultResponseErrorHandler}.
     */
    public void setErrorHandler(ResponseErrorHandler errorHandler)
    {
        if (errorHandler != null)
        {
            this.errorHandler = errorHandler;
        }
    }

    /**
     * Return the error handler.
     */
    public ResponseErrorHandler getErrorHandler()
    {
        return this.errorHandler;
    }

    public <T> void exchange(String url, HttpMethodName httpMethod, Map<String, String> headers, Object requestContent,
                             Class<T> responseClazz,IAsyncHttpCallback<T> callback)
    {
        switch (httpMethod)
        {
            case PUT:
                put(url, responseClazz, headers, requestContent,callback);
                break;
            case POST:
                post(url, responseClazz, headers, requestContent,callback);
                break;
            case GET:
                get(url, responseClazz, headers,callback);
                break;
            case DELETE:
                delete(url, headers);
                break;
            default:
                throw new DISClientException("unimplemented.");
        }
    }

    /*
     * HttpClient Get Request
     *
     */
    public <T> void get(String url, Class<T> responseClazz, Map<String, String> headers,IAsyncHttpCallback<T> callback)
    {
        HttpGet request = new HttpGet(url);
        request = this.setHeaders(request, headers);

        HttpMessageConverterExtractor<T> responseExtractor =
                new HttpMessageConverterExtractor<T>(responseClazz, getMessageConverters());
        execute(request, responseExtractor,callback);
    }


    public <T> void post(String url, Class<T> responseClazz, Map<String, String> headers, Object requestBody,IAsyncHttpCallback<T> callback)
    {
        post(url, responseClazz, headers, buildHttpEntity(requestBody),callback);
    }

    public <T> void post(String url, Class<T> responseClazz, Map<String, String> headers, HttpEntity entity,IAsyncHttpCallback<T> callback)
    {
        HttpPost request = new HttpPost(url);
        request = this.setHeaders(request, headers);

        request.setEntity(entity);

        HttpMessageConverterExtractor<T> responseExtractor =
                new HttpMessageConverterExtractor<T>(responseClazz, getMessageConverters());
        execute(request, responseExtractor,callback);
    }

    public <T> void put(String url, Class<T> responseClazz, Map<String, String> headers, Object requestBody,IAsyncHttpCallback<T> callback)
    {
        put(url, responseClazz, headers, buildHttpEntity(requestBody),callback);
    }

    public <T> void put(String url, Class<T> responseClazz, Map<String, String> headers, HttpEntity entity,IAsyncHttpCallback<T> callback)
    {
        HttpPut request = new HttpPut(url);
        request = this.setHeaders(request, headers);

        request.setEntity(entity);

        HttpMessageConverterExtractor<T> responseExtractor =
                new HttpMessageConverterExtractor<T>(responseClazz, getMessageConverters());
        execute(request, responseExtractor,callback);

    }

    public void delete(String url, Map<String, String> headers)
    {

        HttpDelete request = new HttpDelete(url);
        request = this.setHeaders(request, headers);

        execute(request, null,null);
    }

    /**
     * 发送请求并处理响应
     *
     * @param request 请求体
     * @param responseExtractor 响应解析器
     * @return
     */
    protected <T> void execute(final HttpUriRequest request, ResponseExtractor<T> responseExtractor,
                               IAsyncHttpCallback<T> callback)
    {
        httpAsyncClient.execute(request, new FutureCallback<HttpResponse>()
        {
            @Override
            public void completed(HttpResponse httpResponse)
            {
                try
                {
                    handleResponse(httpResponse);
                    if (responseExtractor != null)
                    {
                        callback.onComplete(responseExtractor.extractData(httpResponse),null);
                    }
                    else
                    {
                        callback.onComplete(null,null);
                    }
                }
                catch (IOException e)
                {
                    String resource = request.getURI().toString();
                    String query = request.getURI().getRawQuery();
                    resource = (query != null ? resource.substring(0, resource.indexOf(query) - 1) : resource);
                    RuntimeException ex = new ResourceAccessException("I/O error on " + request.getMethod() + " request for \"" + resource + "\": " + e.getMessage(), e);
                    callback.onComplete(null,ex);
                }
            }

            @Override
            public void failed(Exception ex)
            {
                callback.onComplete(null,ex);
            }

            @Override
            public void cancelled()
            {
                callback.onComplete(null,null);
            }
        });
    }

    /**
     * Handle the given response, performing appropriate logging and invoking the {@link ResponseErrorHandler} if
     * necessary.
     * <p>
     * Can be overridden in subclasses.
     *
     * @param response the resulting {@link HttpResponse}
     * @throws IOException if propagated from {@link ResponseErrorHandler}
     * @since 1.3.0
     * @see #setErrorHandler
     */
    protected void handleResponse(HttpResponse response)
            throws IOException
    {
        ResponseErrorHandler errorHandler = getErrorHandler();
        boolean hasError = errorHandler.hasError(response);

        if (hasError)
        {
            errorHandler.handleError(response);
        }
    }

    /**
     * 设置请求头域
     *
     * @param request 请求体
     * @param headers 需要添加到请求体重的头域
     * @return
     */
    private <T> T setHeaders(T request, Map<String, String> headers)
    {
        if (headers != null)
        {
            for (Map.Entry<String, String> entry : headers.entrySet())
            {
                ((HttpRequest)request).setHeader(entry.getKey(), entry.getValue());
            }
        }
        else
        {
            ((HttpRequest)request).setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
        }

        return request;
    }

    private HttpEntity buildHttpEntity(Object data)
    {

        // TODO 使用 HttpMessageConverter 来实现
        if (data instanceof byte[])
        {
            return new ByteArrayEntity((byte[])data);
        }
        else if (data instanceof String || data instanceof Integer)
        {
            return new StringEntity(data.toString(), "UTF-8");
        }
        else
        {
            return new StringEntity(JsonUtils.objToJson(data), "UTF-8");
        }
    }

    private CloseableHttpAsyncClient getHttpClient()
    {
        RegistryBuilder<SchemeIOSessionStrategy> registryBuilder = RegistryBuilder.<SchemeIOSessionStrategy>create();
        registryBuilder.register("http", NoopIOSessionStrategy.INSTANCE);
        // 指定信任密钥存储对象和连接套接字工厂

        try
        {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            boolean isDefaultTrustedJKSEnabled = disConfig.getIsDefaultTrustedJksEnabled();
            SSLContext sslContext = null;

            // 启用客户端证书校验
            if (isDefaultTrustedJKSEnabled)
            {
                sslContext = SSLContexts.custom().useTLS().loadTrustMaterial(trustStore, null).build();
            }
            else
            {
                // 信任任何链接
                TrustStrategy anyTrustStrategy = new TrustStrategy()
                {
                    @Override
                    public boolean isTrusted(X509Certificate[] x509Certificates, String s)
                            throws CertificateException
                    {
                        return true;
                    }
                };
                sslContext = SSLContexts.custom().useTLS().loadTrustMaterial(trustStore, anyTrustStrategy).build();
            }

            registryBuilder.register("https", new SSLIOSessionStrategy(sslContext,new String[] {"TLSv1.2", "TLSv1.1"}, null, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER));
            PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(
                    new DefaultConnectingIOReactor(IOReactorConfig.DEFAULT),registryBuilder.build());
            connManager.setDefaultMaxPerRoute(disConfig.getMaxPerRoute());
            connManager.setMaxTotal(disConfig.getMaxTotal());
            return HttpAsyncClientBuilder.create()
                    .setConnectionManager(connManager)
                    .build();
        }
        catch (KeyStoreException e)
        {
            throw new RuntimeException(e);
        }
        catch (KeyManagementException e)
        {
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOReactorException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // if (null != in)
            // {
            // try
            // {
            // in.close();
            // }
            // catch (IOException e)
            // {
            // log.error(e.getMessage());
            // }
            // }
        }

    }
}
