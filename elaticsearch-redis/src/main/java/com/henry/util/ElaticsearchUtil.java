package com.henry.util;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * ES 连接demo
 */
public class ElaticsearchUtil {
    public static final String CLUSTER_NAME = "my-application"; //实例名称
    private static final String IP = "XXX";
    //默认端口
    private static final int PORT = 9300;  //端口
    //1.设置集群名称：默认是elasticsearch，并设置client.transport.sniff为true，使客户端嗅探整个集群状态，把集群中的其他机器IP加入到客户端中
    private static Settings settings = Settings
            .settingsBuilder()
            .put("cluster.name", CLUSTER_NAME)
            .put("client.transport.sniff", false)
            .build();
    //创建私有对象
    private static TransportClient client;
    static ThreadLocal<TransportClient> clientThreadLocal = new ThreadLocal<>();

    //取得实例
    public static TransportClient getTransportClient() {
        if (clientThreadLocal.get() != null) {
            return clientThreadLocal.get();
        } else {
            clientThreadLocal.set(client);
            return clientThreadLocal.get();
        }
    }

    //ES2.0版本,静态代码块初始化client，这里实际可以用建造者模式来新建client
    static {
        try {
            client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * 为集群添加新的节点
     *
     * @param name
     */
    public static synchronized void addNode(String name) {
        try {
            ElaticsearchUtil.getTransportClient().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * 按照名称去除集群中的某个节点
     */
    public static synchronized void removeNode(String name) {
        try {
            ElaticsearchUtil.getTransportClient().removeTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(name), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * 新建一个索引和文档类型,id自动生成
     */
    private static void addIndex(String index, String document, String params) {
        IndexResponse indexResponse = ElaticsearchUtil.getTransportClient().prepareIndex(index, document).setSource(params).get();
    }

    /**
     * 新建一个索引和文档类型,id手动指定
     */
    private static void addIndex(String index, String document, String id, String params) {
        IndexResponse indexResponse = ElaticsearchUtil.getTransportClient().prepareIndex(index, document).setId(id).setSource(params).get();
    }


    /**
     * 使用模板指定索引的属性
     */
    private static void addIndexAndTemplate(String index, String document, String id, String params) {
        ElaticsearchUtil.getTransportClient().preparePutIndexedScript().setSource("".getBytes());
    }

    /**
     * 查询索引中的文档,带分页查询
     */
    public static SearchHits queryIndexForMatchResultPage(String index, String type, QueryBuilder queryBuilder, Integer pageNum, Integer pageSize) {
        List<DiscoveryNode> discoveryNodes = ElaticsearchUtil.getTransportClient().connectedNodes();
        SearchResponse response = ElaticsearchUtil.getTransportClient().prepareSearch(index)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setFrom(pageNum)
                .setSize(pageSize)
                .setExplain(true)
                .execute()
                .actionGet();
        SearchHits hits = response.getHits();
        return hits;
    }

    /**
     * 查询索引中的文档（查询可以分为全部匹配、前缀匹配、后缀匹配等等）
     */
    public static SearchHits queryIndexForMatchResult(String index, String type, QueryBuilder queryBuilder) {
        List<DiscoveryNode> discoveryNodes = ElaticsearchUtil.getTransportClient().connectedNodes();
        SearchResponse response = ElaticsearchUtil.getTransportClient().prepareSearch(index)//设置要查询的索引(index)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setExplain(true)
                .execute()
                .actionGet();
        SearchHits hits = response.getHits();
        return hits;
    }

    /**
     * 按照索引、类型、id查询
     *
     * @return json字符串
     */
    public static String getResultById(String index, String type, String id) {
        GetResponse response = ElaticsearchUtil.getTransportClient().prepareGet(index, type, id)
                .setOperationThreaded(false)
                .get();
        return response.getSourceAsString();
    }

    /**
     * 删除某个文档记录,返回被删除文档的id
     */
    public static String deleteIndex(String index, String type, String id) {
        DeleteResponse deleteResponse = ElaticsearchUtil.getTransportClient().prepareDelete(index, type, id).get();
        return deleteResponse.getId();
    }

    /**
     * 更新某个文档内容
     */
    public static String updateDocument(String index, String type, String id, String doc) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(doc);
        UpdateResponse updateResponse = null;
        try {
            updateResponse = client.update(updateRequest).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return updateResponse.getId();
    }

    /**
     * 获取某个索引下对应文档的所有数据的总数量,2.1已经不推荐此方法的使用
     */
    @Deprecated
    public static Long getTotalCount(String index, String type) {
        CountResponse response = ElaticsearchUtil.getTransportClient().prepareCount(index)
                .setTypes(type)
                .execute()
                .actionGet();
        return response.getCount();
    }

    /**
     * 批量按照id获取文档
     */
    public static List<String> getMultiResultbyIds(String index, String type, String... ids) {
        MultiGetResponse multiGetItemResponses = ElaticsearchUtil.getTransportClient().prepareMultiGet().add(index, type, ids).get();
        List<String> res = new ArrayList<>();
        for (MultiGetItemResponse multiGetItemResponse : multiGetItemResponses) {
            GetResponse response = multiGetItemResponse.getResponse();
            if (response.isExists()) {
                res.add(response.getSourceAsString());
            }
        }
        return res;
    }

    /**
     * 按照传入的fieldName范围查询
     * 需要别的查询方式可以继续构建
     *
     * @return
     */
    public static QueryBuilder getRangeQueryBuilder(String fieldName, int from, int to) {
        QueryBuilder qu = QueryBuilders.rangeQuery(fieldName)
                .gt(from)
                .lt(to);
        return qu;
    }
}