package myApacheStorm;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SimplePrintBolt extends BaseRichBolt{
    private OutputCollector collector;
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    public void execute(Tuple input) {
        // TODO Auto-generated method stub
        String tweet = input.getStringByField("TweetStream");
        System.out.println("Bolt:	"+tweet);
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector=collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }
}
