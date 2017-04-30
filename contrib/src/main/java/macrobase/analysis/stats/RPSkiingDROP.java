package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.RPSkiingOptimizer;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RPSkiingDROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(RPSkiingDROP.class);

    private final MBStream<Datum> output;
    int currNt;
    int iter;

    RPSkiingOptimizer rpOpt;
    Stopwatch sw;

    Map<String, Long> times;

    double lbr;

    public RPSkiingDROP(MacroBaseConf conf, double qThresh, double lbr){
        iter = 0;
        currNt = 0;
        rpOpt = new RPSkiingOptimizer(qThresh);
        sw = Stopwatch.createUnstarted();

        times = new HashMap<>();

        this.lbr = lbr;

        output = new MBStream<>();
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
    }

    public Map<String,Map<Integer, Double>> genBasePlots(List<Datum> records){
        rpOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", rpOpt.getM(), rpOpt.getN());
        rpOpt.preprocess();
        log.debug("Processed Data");

        log.debug("Beginning RP base run");
        return rpOpt.computeLBRs();
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }

    public Map<Integer, double[]> getLBR() { return rpOpt.getLBRList();}

    public Map<Integer, Double> getTime(){
        return rpOpt.getTrainTimeList();
    }

    public Map<Integer, Integer> getKList(){
        return rpOpt.getKList();
    }

    public Map<Integer, Integer> getKItersList(){
        return rpOpt.getKItersList();
    }

}
