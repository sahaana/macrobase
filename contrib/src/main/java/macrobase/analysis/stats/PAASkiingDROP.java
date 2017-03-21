package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PAASkiingOptimizer;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PAASkiingDROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(PAASkiingDROP.class);

    private final MBStream<Datum> output;
    int currNt;
    int iter;

    PAASkiingOptimizer paaOpt;
    Stopwatch sw;

    Map<String, Long> times;

    int maxNt;
    double epsilon;
    double lbr;

    public PAASkiingDROP(MacroBaseConf conf, int maxNt, double epsilon, double lbr, int b, int s){
        iter = 0;
        currNt = 0;
        paaOpt = new PAASkiingOptimizer(epsilon, b, s);
        sw = Stopwatch.createUnstarted();

        times = new HashMap<>();

        this.maxNt = maxNt;
        this.epsilon = epsilon;
        this.lbr = lbr;

        output = new MBStream<>();
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
    }

    public Map<Integer, Double> genBasePlots(List<Datum> records) {
        paaOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", paaOpt.getM(), paaOpt.getN());
        paaOpt.shuffleData();
        log.debug("Shuffled Data");
        paaOpt.preprocess();
        log.debug("Processed Data");
        currNt = paaOpt.getNextNt(iter, currNt, maxNt);
        log.debug("Beginning PAA base run");
        paaOpt.fit(currNt);
        //sw.start();
        //paaOpt.setTrainTimeList(currNt, (double) sw.elapsed(TimeUnit.MILLISECONDS));

        return paaOpt.computeLBRs();
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }

    public Map<Integer, double[]> getLBR() { return paaOpt.getLBRList();}

    public Map<Integer, Double> getTime(){
        return paaOpt.getTrainTimeList();
    }

    public Map<Integer, Integer> getKList(){
        return paaOpt.getKList();
    }

    public Map<Integer, Integer> getKItersList(){
        return paaOpt.getKItersList();
    }

}
