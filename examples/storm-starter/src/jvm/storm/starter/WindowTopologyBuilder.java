package storm.starter;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import storm.starter.bolt.BaseWindowBolt;


/**
 * Created by Pradheep on 6/18/15.
 */


public class WindowTopologyBuilder extends TopologyBuilder {


    public BoltDeclarer setBolt(String id, BaseWindowBolt bolt, Number parallelism_hint) throws Exception {

        Class<?> x = bolt.getClass();
        //System.out.println(x);
        Class[] interfaces = x.getInterfaces();
        BoltDeclarer obj = null;
        for (Class i : interfaces) {
            //System.out.println(i.toString());
            if (i.toString().equals("interface storm.starter.Interfaces.IWindowBolt")) {
                obj = super.setBolt(id, bolt, parallelism_hint);
                break;
            }
            else throw new Exception(x + "did not implement IWindow Interface");
        }
        return obj;
    }

    public BoltDeclarer setBolt(String id, BaseRichBolt bolt, Number parallelism_hint) throws Exception {

        Class<?> x = bolt.getClass();
        //System.out.println(x);
        Class[] interfaces = x.getInterfaces();
        BoltDeclarer obj = null;
        if(interfaces.length >=1) {
            for (Class i : interfaces) {
                //System.out.println(i.toString());
                if (i.toString().equals("interface storm.starter.Interfaces.IWindowBolt")) {
                    obj = super.setBolt(id, bolt, parallelism_hint);
                    break;
                } else throw new Exception(x + "did not implement IWindow Interface");
            }
        }
        else
            throw new Exception(x + "did not implement IWindow Interface");
        return obj;
    }
}

