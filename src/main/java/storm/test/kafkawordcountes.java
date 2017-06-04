package storm.test;

/**
 * Created by admin on 2017/6/4.
 */

import net.sf.json.JSONObject;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.spout.SchemeAsMultiScheme;
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
 *  从kafka读取句子，然后根据空格进行分词，最终统计 单词个数 存入es
 */
public class kafkawordcountes {

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
        private  int iwrite = 0;
        public void prepare(Map map, TopologyContext topologyContext) {

        }

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String word = tuple.getString(0);
            iwrite = iwrite + 1 ;
            if (iwrite % 20 == 0){
                iwrite = 0;
                ShowDebugMessage("--------------start ssssssssssssssssssss bolt");
                for(String tmp: _counts.keySet())
                {
                    UUID msgId = UUID.randomUUID();
                    JSONObject esjson = new JSONObject();
                    esjson.put("msg",tmp + ":" + _counts.get(tmp));
                    String document = esjson.toString();
                    basicOutputCollector.emit(new Values(document,"wordcount-index", "wordcount", String.valueOf(msgId)));

                }

                ShowDebugMessage("--------------sssssssssssssssssssssssssssssssssssss bolt");
            }
            int count ;
            if(_counts.containsKey(word)){
                count = _counts.get(word);
            }else{
                count = 0;
            }
            count++;
            ShowDebugMessage(word + ":" + count);
            _counts.put(word,count);
           // basicOutputCollector.emit(new Values(word,count));

        }

        public void cleanup() {
            //当关闭时调用此方法将计算保存到数据库中或者输出打印
            ShowDebugMessage("--------------close bolt");
            for(String tmp: _counts.keySet())
            {
               // ShowDebugMessage(tmp + " " + _counts.get(tmp));
               //  insert(tmp,_counts.get(tmp));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("source", "index", "type", "id"));
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
        // 需要zk地址
        //http://www.cnblogs.com/difeng/archive/2016/01/03/5097220.html

        String topologyName = "topology";
        TopologyBuilder  builder = new TopologyBuilder();
        int iStorm = 0;
        String kafkaTopic = "topicname";
        String ZOOKEEPER_HOSTS = "localhost:2181,localhost1:2181";

        String groupId = "wordcount";
        String zkRoot = String.format("/%s_%s", kafkaTopic, topologyName);
        // 静态ip绑定
        // http://blog.csdn.net/tonylee0329/article/details/43016385

        BrokerHosts hosts = new ZkHosts(ZOOKEEPER_HOSTS);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, zkRoot, groupId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        EsConfig esConfig = new EsConfig("http://localhost:9200"); //使用默认映射 source index, type,id
        EsIndexBolt elasticSearchBolt = new EsIndexBolt(esConfig);


        // set spout
        builder.setSpout("spout", kafkaSpout, 2);
        builder.setBolt("split", new SplitSentenceBolt(), 5).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 10).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("save", elasticSearchBolt, 3).fieldsGrouping("count", new Fields("index"));

        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            kafkawordcountes.ShowDebugMessage("-------after submit the topology---");
            Thread.sleep(100000); //暂停一段时间 就会自动终止
            cluster.killTopology(topologyName);
            kafkawordcountes.ShowDebugMessage("-------this is end for this topology----");
            cluster.shutdown();
        }
    }

}

