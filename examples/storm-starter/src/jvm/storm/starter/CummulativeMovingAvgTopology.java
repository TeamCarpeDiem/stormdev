package storm.starter;

/**
 * Created by Pradheep on 6/2/15.
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.bolt.MovingAverageBolt;
import storm.starter.spout.RandomMessageSpout;

import java.io.File;

import static java.lang.System.exit;

public class CummulativeMovingAvgTopology {
    public static void main(String[] args) throws Exception {
        final Logger LOG = Logger.getLogger(CummulativeMovingAvgTopology.class.getName());
        System.gc();
        WindowObject wObject;

        String log4jConfigFile = System.getProperty("user.dir")
                + File.separator + "log4j.properties";

        PropertyConfigurator.configure(log4jConfigFile);
        WindowTopologyBuilder builder;

        Config conf = new Config();
        conf.setDebug(false);

        wObject = new WindowObject("Sliding", 10000,2500, false);
        //wObject = new WindowObject("Sliding", 20, 5, true); //Uncomment this line for time based window and comment the previois line
        builder = new WindowTopologyBuilder();
        builder.setSpout("RandomMessage", new RandomMessageSpout(), 10);
        builder.setBolt("Sliding", wObject.createWindow() ,1).shuffleGrouping("RandomMessage");
        builder.setBolt("Average", new MovingAverageBolt(), 1).shuffleGrouping("Sliding","dataStream")
                .shuffleGrouping("Sliding", "mockTickTuple");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            LOG.info("Topology Created");
            Utils.sleep(30000);
            cluster.killTopology("test");
            LOG.info("Topology Killed");
            cluster.shutdown();
           exit(0);
        }
    }
}


