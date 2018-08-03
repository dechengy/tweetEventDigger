package myApacheStorm;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class App 
{
    private static final String TWEETSTREAM_SPOUT_ID = "TweetStream-Spout1";
    private static final String SIMPLEPRINT_BOLT_ID = "SimplePrint-Bolt";
    private static final String TOPOLOGY_NAME = "Tweet-topology";

    public static void main ( String[] args )throws Exception
    {
        TweetStreamSpout spout1 = new TweetStreamSpout();
        SaveTweetBolt bolt = new SaveTweetBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(TWEETSTREAM_SPOUT_ID, spout1, 1);
        builder.setBolt(SIMPLEPRINT_BOLT_ID, bolt,1).shuffleGrouping(TWEETSTREAM_SPOUT_ID);
//
        Config config = new Config();
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

//        Utils.sleep(3000);
//        cluster.killTopology(TOPOLOGY_NAME);
//        cluster.shutdown();
    }
}
