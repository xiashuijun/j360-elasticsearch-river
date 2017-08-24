package me.j360.elasticsearch.river;

import me.j360.elasticsearch.river.manager.ElasticClient;
import org.junit.Test;

/**
 * Package: me.j360.elasticsearch.river
 * User: min_xu
 * Date: 2017/8/24 下午8:08
 * 说明：
 */
public class ElasticSearchTest {


    @Test
    public void addSourceTest() {
        ElasticClient elasticClient = new ElasticClient();
        elasticClient.create();
    }
}
