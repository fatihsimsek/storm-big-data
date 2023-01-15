import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordReaderSpout implements IRichSpout, CompletableSpout {

    private static final long serialVersionUID = 441966625018520917L;
    private SpoutOutputCollector collector;

    private String[] sentences = {
            "Hello World",
            "Apache Storm",
            "Big Data",
            "Test Big Data",
            "Machine Learning",
            "Hello World",
            "Test",
            "Data"
    };

    boolean isCompleted;
    String fileName;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {
    }

    public void activate() {
    }

    public void deactivate() {
    }

    public void nextTuple() {
        if (!isCompleted) {
            for (String sentence: sentences) {
                for (String word: sentence.split(" ")) {
                    this.collector.emit(new Values(word));
                }
            }
            isCompleted = true;
        } else {
            this.close();
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));

    }

    public Map< String, Object > getComponentConfiguration() {
        return null;
    }

    @Override
    public boolean isExhausted() {
        return this.isCompleted;
    }
}
