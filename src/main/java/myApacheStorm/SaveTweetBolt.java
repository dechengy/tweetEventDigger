package myApacheStorm;

import java.util.Map;
import java.sql.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONArray;
import org.json.JSONObject;

public class SaveTweetBolt extends BaseRichBolt{
    private OutputCollector collector;
    private JSONObject obj;
    private String excapeString = "";
    private JSONArray array;
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    public void execute(Tuple input) {
        // TODO Auto-generated method stub
        String tweet = input.getStringByField("TweetStream");
        tweet=tweet.replace("StatusJSONImpl","");
        excapeString = tweet.replace("\\\"","\"");
        excapeString = tweet.replace("\\\'","\'");
        System.out.println(tweet);
        obj = new JSONObject(excapeString);
        if(array.length()<10){
            array.put(obj);
            System.out.println(obj.toString());
        }
        else{
            insertIntoDB(array);
            array = new JSONArray();
        }
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector=collector;
        array =  new JSONArray();
    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }
    public void insertIntoDB(JSONArray array){
        String userid = "";
        String username="";
        String id = "";
        String latitude = "";
        String longitude = "";
        String text = "";

        String sql = "";

        Connection conn = null;
        Statement stmt = null;

        try{
            Class.forName(Conf.JDBC_DRIVER);
            conn = DriverManager.getConnection(Conf.DB_URL+"?useSSL=FALSE&serverTimezone=UTC",Conf.user,Conf.password);
            stmt=conn.createStatement();
            for(int i =0;i<array.length();i++){
                obj = array.getJSONObject(i);
                id = obj.getString("ID");
                userid = obj.getString("UserID");
                username = obj.getString("UserName");
                latitude = obj.getString("Latitude");
                longitude = obj.getString("Longitude");
                text = obj.getString("Text");

                sql = "INSERT INTO tweet (Id,Userid,Username,Latitude,Longitude,Text) VALUES "+
                        "("+id+","+userid+",'"+username+"',"+latitude+","+longitude+",'"+text+"')";
                System.out.println(sql);
                stmt.executeUpdate(sql);
            }
            stmt.close();
            conn.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
