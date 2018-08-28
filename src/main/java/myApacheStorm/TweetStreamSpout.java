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

import twitter4j.*;
import twitter4j.auth.AccessToken;

public class TweetStreamSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private LinkedList<Status> queue = null;
    private TwitterStream twitterStream;

    public static final String customer_key = "ujqGAvWCv9C14893iDGd8aUfb";
    public static final String secret_key = "7VQIxQ8cQJ0mQQwre9nRG0uJ2LEBRlHmOcPUpjHPHO9DmsoDTE";
    public static final String access_token = "985020272716595200-uaGcLS4bODn5cP2MMl2NiRRWyKbBoyS";
    public static final String secret_access_token = "CWgEX0NxeIVMpM1syspP7ZzvQeMgdGncTekIkgiGvECn9";

    private long current_id=0;
    private long last_id=0;
    private String output = "";
    private String userid = "";
    private String username="";
    private GeoLocation geolocation;
    private String latitude="";
    private String longitude="";
    private String text="";
    private String date = "";


    public static void main(String[] args) {
        // TODO Auto-generated method stub
    }

    public void nextTuple() {
        // TODO Auto-generated method stub
        if (queue.isEmpty()) {
            System.out.println("queue is empty");
        } else {
            Status current = queue.getLast();
            current_id= current.getId();
            if(last_id==current_id) {
                Utils.sleep(10);
            }else
            {
//                System.out.println(current.getCreatedAt().toString());
                userid=String.valueOf(current.getUser().getId());
                username=current.getUser().getName();
                date=current.getCreatedAt().toString();
                try{
                    geolocation = current.getGeoLocation();
                    latitude = String.valueOf(geolocation.getLatitude());
                    longitude = String.valueOf(geolocation.getLongitude());
                }catch (Exception e){
                    latitude = "";
                    longitude = "";
                }
                text = current.getText().replace("\'","");
                text= text.replace("\"","");
                text = text.replace("\n","");
                text = text.replace("\t","");
                text = text.replace("\\","");
                text = text.replace("/","");
                output = "{\"UserID\":\"" + userid + "\"," +
                        "\"UserName\":\"" + username + "\"," +
                        "\"Latitude\":\"" + latitude + "\"," +
                        "\"Longitude\":\"" + longitude + "\"," +
                        "\"ID\":\"" + current_id + "\"," +
                        "\"Date\":\"" + date + "\"," +
                        "\"Text\":\"" + text + "\"}";
                if(!longitude.equals("")){
                    collector.emit(new Values(output));
                }
                last_id=current_id;
            }
        }
        Utils.sleep(10);
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
                Utils.sleep(10);
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
        System.out.println("Listener Added");
        FilterQuery filterQuery = new FilterQuery();
        double[][] location = {{144.5937,-38.4339},{145.5125,-37.5113}};
        filterQuery.locations(location);
        twitterStream.filter(filterQuery);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("TweetStream"));
    }
}
