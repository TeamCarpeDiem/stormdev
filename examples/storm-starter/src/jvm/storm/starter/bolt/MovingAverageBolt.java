package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.Interfaces.IWindowBolt;

import java.util.Map;

/**
 * Created by sachin on 6/11/15. //Test bolt to verify tumbling window
 */
public class MovingAverageBolt extends BaseRichBolt implements IWindowBolt{
    final static Logger LOG = Logger.getLogger(MovingAverageBolt.class.getName());
    OutputCollector _collector;
    //static WindowObject wObject;
    int count;
    double cma;
    long counter;
    int length;
    String sample;
    long start =System.currentTimeMillis();
    public MovingAverageBolt()
    {
        counter=0;
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
            LOG.info("The tuple data is:: " + tuple.getString(0));
            LOG.info("Window Avg is::" + avg + "    Window Total::" + cma + "   Count::" + count + "   Length::" + length + "   Sample::" + sample);
            LOG.info("time between mock tuples:: "+(System.currentTimeMillis()-start) );
            start = System.currentTimeMillis();
            count = 0;
            cma = 0;
        }
        else {
            cma = cma + 1;//tuple.getInteger(0);
            length = tuple.getString(0).length();
            sample = tuple.getString(0).substring(0,13);
            count++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Average"));
    }

    public boolean isMockTick(Tuple tuple) {
        return tuple.getSourceStreamId().equals("mockTickTuple");
    }
}
