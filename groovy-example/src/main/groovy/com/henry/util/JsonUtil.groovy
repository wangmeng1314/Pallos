package com.henry.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
 * Json工具类
 */
class JsonUtil {
    private final static EMPTY_JSON = "{}"
    /**
     * 转换json为Map
     * @param jsonInput
     * @return
     */
    public static Map parseJsonToMap(String jsonInput) {
        /*解析一段json为Map*/
        def slurper = new JsonSlurper()
        def jsonOutput = slurper.parseText(jsonInput)
        assert jsonOutput instanceof Map
        /*支持List*/
        return jsonOutput
    }
    /**
     * 格式化json后输出
     * @param jsonInput
     * @return
     */
    public static String printPretty(String jsonInput) {
        def jsonOutput = JsonOutput.toJson(jsonInput)
        def prettyJson = JsonOutput.prettyPrint(jsonOutput)
        return prettyJson.toString()
    }
    /**
     * 解析对象到json
     * @param inPut
     * @return
     */
    public static String parseObjectToJson(Object inPut) {
        if (inPut == null) {
            return EMPTY_JSON
        }
        def json = JsonOutput.toJson(inPut)
        return json
    }
}