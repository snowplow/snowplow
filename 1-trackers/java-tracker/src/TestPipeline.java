//File: TestCase.java
//Author: Kevin Gleason
//Date: 6/4/14
//Use: Test case scenario for Javaplow tracker


import javaplow.Tracker;import javaplow.TrackerC;import org.json.JSONException;

import java.io.IOException;
import java.lang.InterruptedException;import java.lang.String;import java.lang.System;import java.lang.Thread;import java.net.URISyntaxException;
import java.util.Random;

// Nothing to do with a node, just simulating random numbers and data

public class TestPipeline {
    //Instance Variables
    private Tracker t1;
    private double SUCCESS_RATE;
    private double NODE_POWER;
    private String node_id;

    TestPipeline(String node_id){
        this.t1 = new TrackerC("d2pac8zn4o1kva.cloudfront.net", "Data Pipeline MW01 Success and CPU",
                node_id, "com.saggezza", true, true);
        this.SUCCESS_RATE = getPercent() + 25.0;
        this.NODE_POWER = getPercent() - 45.0;
        this.node_id = node_id;
    }

    public void runNodeIterations(int n)throws JSONException, IOException, URISyntaxException{
        TrackerC.debug=true;
        String context = "{'Company':'KevinG inc.', 'Data Work ID':'KGi 001'}";
        for (int i=0; i<n; i++){
            double CPU = getUsageCPU();
            boolean succeeded = succeedOrFail(CPU);
            t1.track_struct_event("Pipeline Work", "Node Processing","Succeed and CPU", succeeded ? "OK" : "FAILED",
                    (int) CPU,"com.saggezza",context);
            try { Thread.sleep(200 * getRandIntZeroToN(10)); }
            catch (InterruptedException e){}
        }
    }

    public double getPercent(){
        Random r = new Random(); //NEED ID RANGE
        double p = r.nextDouble() * 100;
        return p;
    }

    public double getUsageCPU(){
        double uCPU = getPercent() + this.NODE_POWER;
        if (uCPU > 100)
            uCPU = 100;
        if (uCPU < 5)
            uCPU = 5;
        return uCPU;
    }

    public int getRandIntZeroToN(int n){
        Random r = new Random();
        return r.nextInt(n+1);
    }

    public boolean succeedOrFail(double work){
        return work <= this.SUCCESS_RATE ? true : false;
    }

    public String toString(){
        return "Node: " + this.node_id + "\nSuccess Rate: " + this.SUCCESS_RATE + "\nPower: " + this.NODE_POWER;
    }

    public static void main(String[] args) throws JSONException, IOException, URISyntaxException{
        TestPipeline p1 = new TestPipeline("Node 0001");
        TestPipeline p2 = new TestPipeline("Node 0002");
        TestPipeline p3 = new TestPipeline("Node 0003");
        TestPipeline p4 = new TestPipeline("Node 0004");
        TestPipeline p5 = new TestPipeline("Node 0005");

        System.out.println(p1.toString());
        System.out.println(p2.toString());
        System.out.println(p3.toString());
        System.out.println(p4.toString());
        System.out.println(p5.toString());

        p1.runNodeIterations(20);
        p2.runNodeIterations(20);
        p3.runNodeIterations(20);
        p4.runNodeIterations(20);
        p5.runNodeIterations(20);

    }
}
