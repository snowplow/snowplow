//File: TestCase.java
//Author: Kevin Gleason
//Date: 6/4/14
//Use: Test case scenario for Javaplow tracker


import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

// Nothing to do with a node, just simulating random numbers and data

public class TestPipelineUnstruct {
    //Instance Variables
    private Tracker t1;
    private double SUCCESS_RATE;
    private double NODE_POWER;
    private String node_id;

    TestPipelineUnstruct(String node_id){
        this.t1 = new TrackerC("d2pac8zn4o1kva.cloudfront.net", "Data Pipeline MW03 Struct and Unstruct",
                node_id, "com.saggezza", true, true);
        this.SUCCESS_RATE = getPercent() + 25.0;
        this.NODE_POWER = getPercent() - 45.0;
        this.node_id = node_id;
    }

    public void runNodeIterations(int n)throws JSONException, IOException, URISyntaxException{
        TrackerC.debug=true;
        String context = "{'Company':'KevinG inc.', 'Data Work ID':'KGi 002'}";
        for (int i=0; i<n; i++){
            double CPU = getUsageCPU();
            boolean succeeded = succeedOrFail(CPU);
            t1.track_unstruct_event("Saggezza", "Pipeline Statistics", buildInfo(CPU, succeeded, i), context);
            t1.track_struct_event("Pipeline Work", "Node Processing","Succeed and CPU", succeeded ? "OK" : "FAILED",
                    (int) CPU,"com.saggezza",context);
            try { Thread.sleep(200 * getRandIntZeroToN(10)); }
            catch (InterruptedException e){}
        }
    }

    public String buildInfo(double CPU, boolean succeeded, int i) throws JSONException{
        JSONObject jsonDict = new JSONObject();
        jsonDict.put("CPU", CPU);
        jsonDict.put("Status", succeeded ? "OK" : "FAILED");
        jsonDict.put("Iteration", i);
        return jsonDict.toString();
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
        TestPipelineUnstruct p1 = new TestPipelineUnstruct("Node 0001");
        TestPipelineUnstruct p2 = new TestPipelineUnstruct("Node 0002");
        TestPipelineUnstruct p3 = new TestPipelineUnstruct("Node 0003");
        TestPipelineUnstruct p4 = new TestPipelineUnstruct("Node 0004");
        TestPipelineUnstruct p5 = new TestPipelineUnstruct("Node 0005");

        System.out.println(p1.toString());
//        System.out.println(p1.buildInfo(90,false,3));
        System.out.println(p2.toString());
        System.out.println(p3.toString());
        System.out.println(p4.toString());
        System.out.println(p5.toString());

        p1.runNodeIterations(10);
        p2.runNodeIterations(10);
        p3.runNodeIterations(10);
        p4.runNodeIterations(10);
        p5.runNodeIterations(10);

    }
}
