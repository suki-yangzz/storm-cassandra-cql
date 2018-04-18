package com.hmsonline.trident.cql.example.simpleupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.datastax.driver.core.ConsistencyLevel;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CassandraCqlStateUpdater;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;

public class SimpleUpdateTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleUpdateTopology.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static StormTopology buildTopology() {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();
        SimpleUpdateSpout spout = new SimpleUpdateSpout();
        Stream inputStream = topology.newStream("test", spout);
        SimpleUpdateMapper mapper = new SimpleUpdateMapper();
        inputStream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), new Fields("test"), new CassandraCqlStateUpdater(mapper));
        // inputStream.each(new Fields("test"), new Debug());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        final Config configuration = new Config();
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "127.0.0.1");
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CLUSTER_NAME, "Test Cluster");
        final LocalCluster cluster = new LocalCluster();
        LOG.info("Submitting topology.");
        cluster.submitTopology("cqlexample", configuration, buildTopology());
        LOG.info("Topology submitted.");
        Thread.sleep(600000);
    }
}
