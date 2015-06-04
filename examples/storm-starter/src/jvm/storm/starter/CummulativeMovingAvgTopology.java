package storm.starter;

/**
 * Created by Pradheep on 6/2/15.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import storm.starter.spout.RandomIntegerSpout;

import java.io.File;
import java.util.Map;

//import backtype.storm.testing.TestWordSpout;

public class CummulativeMovingAvgTopology {
    final static Logger LOG = Logger.getLogger(CummulativeMovingAvgTopology.class.getName());
    public static class MovingAverage extends BaseRichBolt {
        OutputCollector _collector;
        int count;
        double cma;

        public MovingAverage()
        {
            count = 0;
            cma = 0;
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            if (isMockTick(tuple))
            {
                LOG.info("~~~Got Mock Tuple");
                double avg = cma / count;
                _collector.emit(tuple, new Values(avg));
                LOG.info("Window Avg is::"+ avg + "    Window Total::" + cma + "   Count::"+ count);
                count = 0;
                cma = 0;
            }
            else {
                cma = cma + tuple.getInteger(0);
                count++;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("Average"));
        }

        protected static boolean isMockTick(Tuple tuple) {
            return tuple.getSourceStreamId().equals("mockTickTuple");
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String log4jConfigFile = System.getProperty("user.dir")
                + File.separator + "log4j.properties";

        PropertyConfigurator.configure(log4jConfigFile);

        LOG.info("Testing Count Based");
        builder.setSpout("RandomInt", new RandomIntegerSpout(), 10);
        builder.setBolt("Tumbling", new TumblingWindow(4000,false),1).shuffleGrouping("RandomInt");
        builder.setBolt("Average", new MovingAverage(), 1).shuffleGrouping("Tumbling","dataStream")
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

        LOG.info("Testing Time Based");
        builder = new TopologyBuilder();
        builder.setSpout("RandomInt", new RandomIntegerSpout(), 10);
        builder.setBolt("Tumbling", new TumblingWindow(5,true),1).shuffleGrouping("RandomInt");
        builder.setBolt("Average", new MovingAverage(), 1).shuffleGrouping("Tumbling","dataStream")
                .shuffleGrouping("Tumbling","mockTickTuple");

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


