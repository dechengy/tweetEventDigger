package myApacheStorm;

import java.util.LinkedList;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

public class TweetStreamSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private LinkedList<Status> queue = null;
    private TwitterStream twitterStream;

    private static final String customer_key = Conf.customer_key;
    private static final String secret_key = Conf.secret_key;
    private static final String access_token = Conf.access_token;
    private static final String secret_access_token = Conf.secret_access_token;

    private String output1;
    private String output2;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
    }

    public void nextTuple() {
        // TODO Auto-generated method stub
//		Status current = queue.poll();
//		if (current==null){
//			System.out.println("Status is empty!");
//			Utils.sleep(100);
//			}
//		else{
//		output = current.getUser() + " : " +current.getText().toLowerCase();
//		collector.emit(new Values(output));
//		Utils.sleep(100);
//		}
        if (queue.isEmpty()) {
            System.out.println("queue is empty");
        } else {
            Status current = queue.getLast();

            output1 = current.getUser().getName() + " : " + current.getText().toLowerCase();
//		System.out.println("Spout:   "+output);
            if (output1 == null) {
                System.out.println("Status is empty");
            } else if (output1.equals(output2)) {
                Utils.sleep(100);
            } else {
                this.collector.emit(new Values(output1));
//			System.out.println("Spout:   "+output1);
                output2 = output1;
                output1 = null;
            }
        }
        Utils.sleep(100);
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        queue = new LinkedList<Status>();
        StatusListener listener = new StatusListener() {

            public void onException(Exception ex) {
                ex.printStackTrace();
                System.out.println("初始化Tweet Stream 失败");

            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                // TODO Auto-generated method stub
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onStatus(Status status) {
                // TODO Auto-generated method stub
                if (status == null) {
                    System.out.println("status is null");
                } else {
                    queue.add(status);
                }
                Utils.sleep(500);
            }

            public void onStallWarning(StallWarning warning) {
                // TODO Auto-generated method stub
                System.out.println("Got stall warning:" + warning);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                // TODO Auto-generated method stub
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                // TODO Auto-generated method stub
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }
        };

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.setOAuthConsumer(customer_key, secret_key);
        twitterStream.setOAuthAccessToken(new AccessToken(access_token, secret_access_token));

        twitterStream.addListener(listener);
        System.out.println("Listener 已经添加");
        twitterStream.sample("en");

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("TweetStream"));
    }
}
