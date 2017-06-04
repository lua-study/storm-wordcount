package storm.test;

/**
 * Created by admin on 2017/6/4.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 *  实现一个最最基本storm
 *  随机生成单词，然后统计 单词个数
 */
public class wordcount {

    public  static class WordSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private  final String[] msgs = new String[]{
                "I have a dream",
                "my dream is to be a data analyst",
                "you kan do waht you are dreaming",
                "don't give up your dreams",
                "it's just so so",
                "we need change the traditional ideas and practice boldly",
                "storm enterprise real time calculation of actual combat",
                "you can be what you want be"
        };
        private  final Random random = new Random();


        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
        }
        //随机生成选择一个句子 发送到下一个bolt
        public void nextTuple() {
            String sentence = msgs[random.nextInt(8)];
            collector.emit(new Values(sentence));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence"));
        }
    }

    //第一个bolt 切分单词
    public static class SplitSentenceBolt implements IBasicBolt {

        public void prepare(Map map, TopologyContext topologyContext) {

        }

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")){  //空格 切分单词，发送到下一个bolt
                basicOutputCollector.emit(new Values(word));
            }
        }
        public void cleanup() {
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
    //统计每个单词数量
    public static  class WordCountBolt implements IBasicBolt{
        private  Map<String, Integer> _counts = new HashMap<String, Integer>();
        public void prepare(Map map, TopologyContext topologyContext) {
        }
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String word = tuple.getString(0);
            int count ;
            if(_counts.containsKey(word)){
                count = _counts.get(word);
            }else{
                count = 0;
            }
            count++;
            ShowDebugMessage(word + ":" + count);
            _counts.put(word,count);
            basicOutputCollector.emit(new Values(word,count));
        }

        public void cleanup() {
            //当关闭时调用此方法将计算保存到数据库中或者输出打印
            ShowDebugMessage("--------------close bolt");
            for(String tmp: _counts.keySet())
            {
                ShowDebugMessage(tmp + " " + _counts.get(tmp));
                //  insert(tmp,_counts.get(tmp));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word","count"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
    public static void ShowDebugMessage(String strMessage)
    {
        System.out.println(strMessage);
    }


    public static void main(String[] args) throws Exception {


        String topologyName = "topology";
        TopologyBuilder  builder = new TopologyBuilder();
        builder.setSpout("spout", new WordSpout(), 2);
        builder.setBolt("split", new SplitSentenceBolt(), 5).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 10).fieldsGrouping("split", new Fields("word"));
        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            wordcount.ShowDebugMessage("-------after submit the topology---");
            //   while(true)
            Thread.sleep(10000); //暂停一段时间 就会自动终止
            cluster.killTopology(topologyName);
            wordcount.ShowDebugMessage("-------this is end for this topology----");
            cluster.shutdown();
        }
    }

}

