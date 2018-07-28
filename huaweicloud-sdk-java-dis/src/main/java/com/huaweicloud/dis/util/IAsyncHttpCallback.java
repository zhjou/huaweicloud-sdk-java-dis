package com.huaweicloud.dis.util;

/**
 * Created by z00382129 on 2018/6/27.
 */
public interface IAsyncHttpCallback<T>
{
    void onComplete(T data, Exception e);
}
