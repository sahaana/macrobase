package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.FFTSkiingOptimizer;
import macrobase.analysis.stats.optimizer.PAASkiingOptimizer;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FFTSkiingDROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(FFTSkiingDROP.class);

    private final MBStream<Datum> output;
    double[][] finalTransform;;
    double[] currLBR;
    int currNt;
    int iter;

    RealMatrix currTransform;
    FFTSkiingOptimizer fftOpt;
    Stopwatch sw;

    Map<String, Long> times;

    int maxNt;
    int procDim;
    double epsilon;
    double lbr;
    int b;
    int s;
    boolean rpFlag;

    public FFTSkiingDROP(MacroBaseConf conf, int maxNt, double epsilon, double lbr, int b, int s, boolean rpFlag){
        iter = 0;
        currNt = 0;
        fftOpt = new FFTSkiingOptimizer(epsilon, b, s);
        sw = Stopwatch.createUnstarted();

        times = new HashMap<>();

        this.maxNt = maxNt;
        this.procDim = 707; //This is an appendix
        this.epsilon = epsilon;
        this.lbr = lbr;
        this.b = b;
        this.s = s;
        this.rpFlag = rpFlag;

        output = new MBStream<>();
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        fftOpt.extractData(records);
        log.debug("Extracted Records");
        fftOpt.shuffleData();
        log.debug("Shuffled Data");
        fftOpt.preprocess(procDim);
        log.debug("Processed Data");
        currNt = fftOpt.getNextNt(iter, currNt, maxNt);
        log.debug("Beginning DROP");
        sw.start();
        log.debug("Iteration {}, {} samples", iter, currNt);
        fftOpt.fit(currNt);
        //fftOpt.test();

        currTransform = fftOpt.getKBin(iter, lbr);
        currLBR = fftOpt.LBRCI(currTransform, fftOpt.getM(), 1.96);//paaOpt.LBRAttained(iter, currTransform); //TODO: this is repetitive. Refactor the getKI things to spit out
        fftOpt.setLBRList(currNt, currLBR);
        fftOpt.setTrainTimeList(currNt, (double) sw.elapsed(TimeUnit.MILLISECONDS));
        fftOpt.setKList(currNt, currTransform.getColumnDimension());
        fftOpt.setKDiff(iter, currTransform.getColumnDimension());
        log.debug("LOW {}, LBR {}, HIGH {}, VAR {} K {}.", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());
        currNt = fftOpt.getNextNt(++iter, currNt, maxNt);

        finalTransform = currTransform.getData();

        int i = 0;
        for (Datum d: records){
            RealVector transformedMetricVector = new ArrayRealVector(finalTransform[i++]);
            output.add(new Datum(d, transformedMetricVector));
        }

    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }

    public Map<Integer, double[]> getLBR() { return fftOpt.getLBRList();}

    public Map<Integer, Double> getTime(){
        return fftOpt.getTrainTimeList();
    }

    public Map<Integer, Integer> getKList(){
        return fftOpt.getKList();
    }

    public Map<Integer, Integer> getKItersList(){
        return fftOpt.getKItersList();
    }

}
