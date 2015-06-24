package storm.starter;

/**
 * Created by Pradheep on 6/2/15.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.WindowTopologyBuilder;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.bolt.MovingAverageBolt;
import storm.starter.bolt.SlidingWindowBolt;
import storm.starter.bolt.TumblingWindow;
import storm.starter.spout.RandomIntegerSpout;
import storm.starter.spout.RandomSentenceSpout;

import java.io.File;

public class CummulativeMovingAvgTopology {
    final static Logger LOG = Logger.getLogger(CummulativeMovingAvgTopology.class.getName());
    public static void main(String[] args) throws Exception {
        //TopologyBuilder builder = new TopologyBuilder();
        WindowTopologyBuilder builder = new WindowTopologyBuilder();
        WindowObject wObject;
       // wObject = new WindowObject(4000, false);

        String log4jConfigFile = System.getProperty("user.dir")
                + File.separator + "log4j.properties";

        PropertyConfigurator.configure(log4jConfigFile);
/*
        LOG.info("Testing Count Based");
        builder.setSpout("RandomInt", new RandomIntegerSpout(), 10);
        builder.setBolt("Tumbling", new TumblingWindow(wObject),1).shuffleGrouping("RandomInt");
        builder.setBolt("Average", new MovingAverageBolt(), 1).shuffleGrouping("Tumbling","dataStream")
                .shuffleGrouping("Tumbling","mockTickTuple");


        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology("test");
            cluster.shutdown();
        }

        Utils.sleep(5000);

*/
        Config conf = new Config();
        conf.setDebug(false);
        LOG.info("Testing Time Based");
        wObject = new WindowObject("sliding",10,2,true);
        //wObject = new WindowObject("tumbling",10,10,true);
        builder.setSpout("RandomInt", new RandomIntegerSpout(), 15);
        builder.setBolt("Tumbling", wObject.CreateWindow(),1).shuffleGrouping("RandomInt");
        //builder.setBolt("Sliding", wObject.CreateWindow(),1).shuffleGrouping("RandomInt");
        builder.setBolt("Average", new MovingAverageBolt(), 1).shuffleGrouping("Sliding","dataStream")
                .shuffleGrouping("Sliding","mockTickTuple");
        //builder.setBolt("Average", new MovingAverageBolt(), 1).shuffleGrouping("Tumbling", "dataStream")
        //        .shuffleGrouping("Tumbling","mockTickTuple");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}


