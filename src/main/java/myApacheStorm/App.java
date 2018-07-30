package myApacheStorm;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
/**
 * Hello world!
 *
 */
public class App 
{
    private static final String TWEETSTREAM_SPOUT_ID = "TweetStream-Spout";
    private static final String SIMPLEPRINT_BOLT_ID = "SimplePrint-Bolt";
    private static final String TOPOLOGY_NAME = "Tweet-topology";

    public static void main ( String[] args )throws Exception
    {
        TweetStreamSpout spout = new TweetStreamSpout();
        SimplePrintBolt bolt = new SimplePrintBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TWEETSTREAM_SPOUT_ID, spout, 1);
        builder.setBolt(SIMPLEPRINT_BOLT_ID, bolt).shuffleGrouping(TWEETSTREAM_SPOUT_ID);

        Config config = new Config();
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

//        Utils.sleep(3000);
//        cluster.killTopology(TOPOLOGY_NAME);
//        cluster.shutdown();
    }
}
