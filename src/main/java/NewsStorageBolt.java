import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;

public class NewsStorageBolt implements IRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(NewsStorageBolt.class);


    private OutputCollector outputCollector;
    private Cluster cassandra;
    private Session session;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        cassandra = Cluster.builder().addContactPoint("localhost").build();
        session = cassandra.connect("breaking_news");
    }

    public void execute(Tuple tuple) {
        LOG.info("Story found");

        String tweet = tuple.getString(0);
        outputCollector.ack(tuple);
        PreparedStatement preparedStatement = session.prepare("INSERT INTO tweets (seen, tweet) VALUES (?, ?)");
        session.execute(preparedStatement.bind(new Timestamp(System.currentTimeMillis()),tweet));
    }

    public void cleanup() {
        cassandra.close();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
