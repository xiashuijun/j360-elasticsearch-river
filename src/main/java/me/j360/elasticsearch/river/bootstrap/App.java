package me.j360.elasticsearch.river.bootstrap;

import me.j360.elasticsearch.river.manager.ElasticClient;

/**
 * Package: me.j360.elasticsearch.river.bootstrap
 * User: min_xu
 * Date: 2017/8/24 下午3:29
 * 说明：
 */
public class App {

    public static void main(String[] args) {
        ElasticClient elasticClient = new ElasticClient();

        elasticClient.createTypeMapping("test","test-1");


        //

    }


}
