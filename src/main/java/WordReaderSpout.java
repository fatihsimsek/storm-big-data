import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordReaderSpout implements IRichSpout, CompletableSpout {

    private static final long serialVersionUID = 441966625018520917L;
    private SpoutOutputCollector collector;

    private List<String> sentences = new ArrayList<>();

    boolean isCompleted;
    String fileName;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.sentences = readInput("input.txt");
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

    private List<String> readInput(String path) {
        List<String> result = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                result.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
