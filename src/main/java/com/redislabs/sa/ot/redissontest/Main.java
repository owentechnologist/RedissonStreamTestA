package com.redislabs.sa.ot.redissontest;

import org.apache.commons.lang3.SerializationUtils;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.api.stream.*;
import org.redisson.config.Config;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;


/**
 * Need to spawn multiple consumerGroups that each target the same Stream
 * Have to ensure that all consumer groups get all the events
 *
 * To check on the Stream using redis-cli -- execute:
 * XINFO STREAM X:dataUpdates
 * XINFO GROUPS X:dataUpdates
 * XINFO CONSUMERS X:dataUpdates group_0
 * XRANGE X:dataUpdates 1650270491021-0 + COUNT 3
 *
 */
public class Main {

    public static final String NON_DEFAULT_GROUP_SIZE = "CHANGEGROUPSIZE";
    public static final String redissonSetName = "Z:dataUpdates";
    public static final String redissonStreamName = "X:dataUpdates";
    public static int playValue = 80;
    public static String providedConnectionString = "redis://127.0.0.1:6379";
    public static String userName = "default";
    public static String passKey = "";
    public static long testSize = 3;// 3 could be 48
    public static int howManyGroups = 2;

    /**
     *
     * Example usage:
     * java com.redislabs.sa.ot.redissontest.Main [connection string] [username] [password]
     * java ...Main "redis://myhappy.server.com:10000" default sw#!ihgso78W5TUEYFGsdfkhus74b4k43298g8WT2
     * @param args
     * @throws Throwable
     */
    public static void main(String[] args) throws Throwable{
        ArrayList<String> tempArgs = new ArrayList<String>();
        if(Arrays.asList(args).contains(NON_DEFAULT_GROUP_SIZE)){
            boolean captureNextArg = false;
            for(String e:args) {
                if(captureNextArg){
                    howManyGroups=Integer.parseInt(e);
                    captureNextArg=false;
                }
                if(e.equalsIgnoreCase(NON_DEFAULT_GROUP_SIZE)){
                    // Expect the next argument to be the value for the size of the groups desired
                    captureNextArg=true;
                }else {
                    tempArgs.add(e);
                }
            }
            // Creating clean version of args with remaining possible arguments
            args = (String[]) tempArgs.toArray(new String[0]);
            System.out.println(args.length);
        }
        System.out.println("$$$ ------ Connecting to "+providedConnectionString);
        testRedisson(initializeConfiguration(args));
        Thread.sleep(3000);
        testRedisson(initializeConfiguration(args));
        writeManyObjectsAsEvents(testSize,initializeConfiguration(args));
        startConsumerGroups(howManyGroups,initializeConfiguration(args));
        writeManyObjectsAsEvents(testSize,initializeConfiguration(args));
    }

    /*
    Redisson delivers more consistent results when multiple config objects are created
    By creating these on demand for each task group, timeouts are avoided.
     */
    static Config initializeConfiguration(String[] args){
        Config config = new Config().setNettyThreads(48);
        if(args.length>1){
            providedConnectionString = args[0];
            userName=args[1];
            passKey=args[2];
            config.useSingleServer().setAddress(providedConnectionString).setClientName(userName).setPassword(passKey);
            //TODO: implement parsing user and password args
        }else if(args.length>0){
            providedConnectionString = args[0];
            config.useSingleServer().setAddress(providedConnectionString);
        }else{
            config.useSingleServer().setAddress(providedConnectionString);
        }
        return config;
    }

    static void startConsumerGroups(int howManyGroups,Config config){
        //Create connection to Redis Server:
        for(int x = 0;x<howManyGroups;x++){
            RedissonConsumerGroup redissonConsumerGroup = new RedissonConsumerGroup(config,x,10000);
            Thread t = new Thread(redissonConsumerGroup);
            t.start();
        }
    }

    static void testRedisson(Config config){
        //Create connection to Redis Server:
        RedissonClient redisson = Redisson.create(config);

        //publish simple event with just: speed=1997 as the Entry:
        System.out.println("Creating an event and adding it to "+redissonStreamName);
        RStream<String, String> stream = redisson.getStream(redissonStreamName);
        System.out.println("Redisson: Adding [ 'speed', "+playValue+ " ] to stream");
        StreamMessageId sid = stream.add("speed", ""+playValue);
        System.out.println("Redisson: Fetching event from stream using id: "+sid);
        Map<StreamMessageId, Map<String, String>> result  = stream.range(sid,sid);
        System.out.println(result);
        Object x = result.entrySet().toArray()[0];
        System.out.println("Redisson: looking at the nested value in the returned Map  ... "+x);

        SomeClazz sc = new SomeClazz();
        System.out.println("Created a new instance of SomeClazz... toString() results in:\n"+sc);
        try{
            //create a ByteArray (Serialized) version of our object suitable for passing around:
            byte[] scBytes = SerializationUtils.serialize(sc);
            System.out.println("After local Serialization/de-Serialization... toString results in:\n"+SerializationUtils.deserialize(scBytes));

            //Get reference to our stream with the expected byte[] type:
            RStream<String, byte[]> stream2 = redisson.getStream(redissonStreamName);

            System.out.println("\tRedisson: Writing the Serialized Object to our Redis Stream \n"+sc);
            StreamMessageId sid2 = stream2.add("someClazz", scBytes);

            System.out.println("Redisson: Fetching lastEvent from stream using id: "+sid2);
            /*
            MAP
            --key: StreamMessageId
            --value: innerMap
                     --key: "someClazz"
                     --value: byte[] (serialized object of type SomeClazz)
            */
            Map<StreamMessageId, Map<String, byte[]>> map  = stream2.range(sid2,sid2);
            System.out.println("Redisson: After reading the Event back from the stream through Redisson it outputs: "+map.toString());
            System.out.println("Redison: Keys in our retrieved Map are: "+map.keySet());

            StreamMessageId nestedEventKey = (StreamMessageId) map.keySet().toArray()[0];
            System.out.println("\tRedison: Fetching the nested Map containing our Serialized Object using: "+nestedEventKey);
            Map<String,byte[]> innerMap = map.get(nestedEventKey);
            SomeClazz clone = SerializationUtils.deserialize(innerMap.get("someClazz"));
            System.out.println("After reading our Object back from the stream through Redisson, toString results in:\n" +clone);

        }catch(Throwable exception){ //maybe we could see ClassCast and IOException?
            exception.printStackTrace();
        }

        System.out.println("For kicks... trying a simple operation using SortedSet");
        playValue += 120;
        System.out.println("Adding   "+playValue+ "  to SortedSet");
        RSortedSet<Object> set = redisson.getSortedSet(redissonSetName);
        set.add(playValue);
        System.out.println("Did you know that Redisson just wrote a List to Redis instead of a SortedSet?");
        System.out.println("Let's try and read that back from the 'SortedSet'  \n"+set.readAll().toArray()[0]);
        //Some strangeness occurs when we rerun the Redisson code without shutting down between runs:
        redisson.shutdown();
    }

    static void writeManyObjectsAsEvents(long howManyObjectsTotal,Config config){
         //Create connection to Redis Server:
        RedissonClient redisson = Redisson.create(config);

        for(int x = 0;x<howManyObjectsTotal;x++){
            SomeClazz sc = new SomeClazz();
            //System.out.println("Created a new instance of SomeClazz... toString() results in:\n"+sc);

            //create a ByteArray (Serialized) version of our object suitable for passing around:
            byte[] scBytes = SerializationUtils.serialize(sc);
            //Get reference to our stream with the expected byte[] type:
            RStream<String, byte[]> stream2 = redisson.getStream(redissonStreamName);
            //System.out.println("\tRedisson: Writing the Serialized Object to our Redis Stream \n" + sc);
            StreamMessageId sid2 = stream2.add("someClazz", scBytes);
        }
        System.out.println("$$$ ------ Completed task of writing "+howManyObjectsTotal+" objects to stream "+redissonStreamName);
        redisson.shutdown();
    }
}

class SomeClazz implements Serializable{
    private static final long serialVersionUID=1L;
    private int someValue = (int) (System.currentTimeMillis()%500);
    private String stringValue = ((System.currentTimeMillis()%2)==0) ? "Happy Days Are Here Again" : "Cloudy & Rainy Sadness Pervades The Globe";

    public void setSomeValue(int sv){
        this.someValue = sv;
    }
    public int getSomeValue(){
        return this.someValue;
    }
    public void setStringValue(String sv){
        this.stringValue = sv;
    }
    public String getStringValue(){
        return this.stringValue;
    }

    public String toString() {
        return "My values are as follows... someValue = " + this.getSomeValue() + "  stringValue = " + this.getStringValue();
    }
}

class RedissonConsumerGroup implements Runnable{
    long instanceStartTime = System.currentTimeMillis();
    RedissonClient redisson = null;
    //String lastUsedEventidKeyName = null;
    RStream<String, byte[]> stream = null;
    String groupName = "";
    String groupMemberId = "only1"; // TODO: add more than one member of each group if necessary
    StreamMessageId lastUsedId = new StreamMessageId(0l);
    long maxNumberOfMessagesBeforeExiting = 0;

    public RedissonConsumerGroup(Config config, int groupId,long maxNumberOfMessagesBeforeExit) {
        this.redisson = Redisson.create(config);
        this.maxNumberOfMessagesBeforeExiting = maxNumberOfMessagesBeforeExit;
        this.stream = redisson.getStream(Main.redissonStreamName);
        this.groupName = "group_"+groupId;
        //  As mentioned elsewhere, no apparent need to track and update the lastUsedId on the consumer-side
        //  lastUsedEventidKeyName=this.groupName+":"+Main.redissonStreamName+":lastUsedId";
        try {
          /*
          * The following commented code was for debugging purposes and appears unnecessary
          * as the Redis Server is tracking delivered messages for the groups:
          try{
                RBucket<String> val = redisson.getBucket(lastUsedEventidKeyName);
                String usedStreamId = val.get();
                this.lastUsedId = new StreamMessageId(Long.parseLong(usedStreamId.split("-")[0]),Long.parseLong(usedStreamId.split("-")[1]));
            }catch (NullPointerException npe){
                // if no record exists in redis... we just go back to our default 0-0 lastUsedId
            }*/
            this.stream.createGroup(groupName, this.lastUsedId);
            System.out.println("Created group... using lastUsedId == "+this.lastUsedId);
        }catch(org.redisson.client.RedisBusyException exception){
            System.out.println(exception.getLocalizedMessage());
            //We could call
            // this.stream.removeGroup(groupName);
            // and recreate it to reset the whole group to a new starting id
            //This appears to be completely unnecessary as the Redis Server is tracking delivered messages for the groups.
        }
        System.out.println("creating group...done");
    }

    public void run(){
        go(this.maxNumberOfMessagesBeforeExiting);
    }

    public void go(long limit){
        long counter = 1;
        try {

            System.out.println("Worker -- " + this.groupName + "_" + this.groupMemberId + " --  Executing first read from group...");
            Map<StreamMessageId, Map<String, byte[]>> map = this.stream.readGroup(groupName, groupMemberId,StreamReadGroupArgs.neverDelivered().count(1).timeout(Duration.ofSeconds(15)));
            System.out.println("Worker -- " + this.groupName + "_" + this.groupMemberId + " -- Executing first read from group... done.");
            processEvent(map);
            while (System.currentTimeMillis() < (this.instanceStartTime+60000) && counter < limit) {
                map = stream.readGroup(groupName, groupMemberId, StreamReadGroupArgs.neverDelivered().count(1).timeout(Duration.ofSeconds(15)));
                if(processEvent(map)) {
                    counter++;
                }
            }
        }catch(Throwable t){
            System.out.println(t.getLocalizedMessage());
            redisson.shutdown();
            System.out.println("\n\nWorker -- "+this.groupName+"_"+this.groupMemberId+" COUNTER REACHED "+counter);
            counter += maxNumberOfMessagesBeforeExiting; //<-- exit while loop
        }
        //be polite:
        System.out.println("\n\nWorker -- "+this.groupName+"_"+this.groupMemberId+" COUNTER REACHED "+counter);
        redisson.shutdown();
    }

    //return true if successful processing of an actual event...
    boolean processEvent(Map<StreamMessageId, Map<String, byte[]>> map){
        boolean wasGood=false;
        System.out.println("Worker -- "+this.groupName+"_"+this.groupMemberId+" -- >> processing event "+map.keySet());
        if(map.keySet().size()>0) {
            StreamMessageId nestedEventKey = (StreamMessageId) map.keySet().toArray()[0];
            /*
            * updating the lastUsedId is unnecessary as the redis server is keeping track for us...
            *
            this.lastUsedId = nestedEventKey;
            RBucket<String> val = redisson.getBucket(lastUsedEventidKeyName);
            val.set(this.lastUsedId.toString());
            */
            Object omap = map.get(nestedEventKey);
            Map<String, byte[]> innerMap = map.get(nestedEventKey);
            if(innerMap.containsKey("someClazz")) {
                SomeClazz clone = SerializationUtils.deserialize(innerMap.get("someClazz"));
                if(Main.howManyGroups==2) {
                    System.out.println("\n\t^^^ SUCCESS! -- " + this.groupName + "_" + this.groupMemberId + " -- >> " + nestedEventKey + "\n" + clone);
                }
                wasGood=true;
            }else{
                if(Main.howManyGroups==2) {
                    System.out.println("\n~~~ SUCCESS! -- " + this.groupName + "_" + this.groupMemberId + " -- >> processing event success: " + omap);
                }
                wasGood=true;
            }
        }
        return wasGood;
    }

}