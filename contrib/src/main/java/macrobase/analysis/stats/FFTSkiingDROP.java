package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.FFTSkiingOptimizer;
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

    int procDim;
    double epsilon;
    double lbr;

    public FFTSkiingDROP(MacroBaseConf conf,  double epsilon, double lbr){
        iter = 0;
        currNt = 0;
        fftOpt = new FFTSkiingOptimizer(epsilon);
        sw = Stopwatch.createUnstarted();

        times = new HashMap<>();

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


    public Map<Integer, Double> genBasePlots(List<Datum> records){
        fftOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", fftOpt.getM(), fftOpt.getN());
        fftOpt.shuffleData();
        log.debug("Shuffled Data");
        fftOpt.preprocess();
        log.debug("Processed Data");
        currNt = fftOpt.getNextNt(iter, currNt);
        log.debug("Beginning FFT base run");
        fftOpt.fit(currNt);
        //sw.start();
        //fftOpt.setTrainTimeList(currNt, (double) sw.elapsed(TimeUnit.MILLISECONDS));

        return fftOpt.computeLBRs();

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
