package me.j360.elasticsearch.river.manager;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

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

    public ElasticClient() {
        String address = "xxx.xx.xx.xx";
        Settings settings = Settings.builder().put("cluster.name","fotoplace-qa").build();
        client = null;
        try {
            client = new TransportClient.Builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), 9300));
        } catch (UnknownHostException e) {
            log.error("初始化client失败:{}",address,e);
        }
        adminClient = client.admin();
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







}
