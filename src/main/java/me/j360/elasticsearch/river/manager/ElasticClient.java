package me.j360.elasticsearch.river.manager;

import lombok.extern.slf4j.Slf4j;
import me.j360.elasticsearch.river.enums.ActionEnum;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Package: me.j360.elasticsearch.river.manager
 * User: min_xu
 * Date: 2017/8/24 下午3:26
 * 说明：
 */

@Slf4j
public class ElasticClient {

    private static AdminClient adminClient;
    private static Client client;
    private static BulkProcessor bulkProcessor;

    public ElasticClient() {
        String address = "123.59.27.210";
        Settings settings = Settings.builder().put("cluster.name","fotoplace-qa").build();
        client = null;
        try {
            client = new TransportClient.Builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), 9300));
        } catch (UnknownHostException e) {
            log.error("初始化client失败:{}",address,e);
        }
        adminClient = client.admin();

        //需要添加close钩子
        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

            }
        }).setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.KB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100),3))
                .build();
    }

    /**
     * create index and type
     * @param index
     * @param type
     */
    public void createMapping(String index, String type) {
        adminClient.indices().prepareCreate(index)
                .addMapping(type, "{\n" +
                        "    \""+type+"\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"message\": {\n" +
                        "          \"type\": \"string\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }")
                .get();
    }


    /**
     * update type in exist index
     * @param index
     * @param type
     */
    public void createTypeMapping(String index, String type) {
        adminClient.indices().preparePutMapping(index)
                .setType(type)
                .setSource("{\n" +
                        "    \""+type+"\":{\n" +
                        "        \"properties\": {\n" +
                        "            \"name\": {\n" +
                        "                \"type\": \"string\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}")
                .get();
    }




    public void create() {

    }

    public void update() {

    }

    public void delete() {

    }

    public void bulkRequest(ActionEnum action, String index, String type, String id, String source) {

        switch (action) {
            case INSERT:
                bulkProcessor.add(new IndexRequest(index, type, id).source(source));
            case UPDATE:
                update();
            case DELETE:
                bulkProcessor.add(new DeleteRequest(index, type, id));
                default:
        }
    }


    public void close() {
        bulkProcessor.close();
    }
}
