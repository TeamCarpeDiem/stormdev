package storm.starter.Interfaces;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Pradheep on 6/18/15.
 */
public interface IWindowBolt {
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void execute(Tuple input);

    void declareOutputFields(OutputFieldsDeclarer declarer);

    boolean isMockTick(Tuple tuple);
}