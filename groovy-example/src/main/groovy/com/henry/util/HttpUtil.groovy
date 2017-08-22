package com.henry.util

import groovyx.net.http.ContentType
import groovyx.net.http.HttpResponseDecorator
import groovyx.net.http.RESTClient

/**
 * HTTP请求工具类
 */
class HttpUtil {

    def static rest = new RESTClient()
    def static paramsGetMap = [uri: '', contentType: '']
    def static paramsPostMap = [uri: '', contentType: '', body: '']
    static String name

    /**
     * <p>发起GET请求</p>
     * @param uri
     * @param contentType
     * @return 返回response包装类
     */
    public static synchronized HttpResponseDecorator getJson(String uri, ContentType contentType) {
        paramsGetMap.uri = uri
        paramsGetMap.contentType = contentType
        HttpResponseDecorator getRes = rest.get(paramsGetMap)
        return getRes
    }
    /**
     * <p>发起POST请求</p>
     * @param uri 请求的地址
     * @param contentType 内容类型
     * @param body 请求体内容
     * @return 返回response包装类
     */
    public static synchronized postJson(String uri, ContentType contentType, String body) {
        paramsPostMap.contentType = contentType
        if (contentType == null) {
            contentType = ContentType.JSON
        }
        paramsPostMap.uri = uri
        paramsPostMap.body = body
        def post = rest.post(uri: uri, contentType: contentType, body: body)
        return post
    }
}
