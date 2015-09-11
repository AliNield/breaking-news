import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.UUID;

public class Main {

    public static final String BREAKING_NEWS_TOPIC = "breaking_news";
    public static final Logger LOG = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        LOG.info("Setting up");

        KafkaSpout kafkaSpout = getKafkaSpout();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("kafka", kafkaSpout);
        topologyBuilder.setBolt("news_storage", new NewsStorageBolt()).shuffleGrouping("kafka");

        Config config = new Config();
        config.setDebug(true);
        StormSubmitter.submitTopology("breakingNews", config, topologyBuilder.createTopology());
    }

    private static KafkaSpout getKafkaSpout() {
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, BREAKING_NEWS_TOPIC, "/" + BREAKING_NEWS_TOPIC, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        LOG.info("created kafka spout");

        return new KafkaSpout(spoutConfig);
    }

}
