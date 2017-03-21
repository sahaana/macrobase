package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PCASkiingOptimizer;
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

public class PCASkiingDROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(PCASkiingDROP.class);

    private final MBStream<Datum> output;
    double[][] finalTransform;;
    double[] currLBR;
    int currNt;
    int iter;

    RealMatrix currTransform;
    PCASkiingOptimizer pcaOpt;
    Stopwatch sw;

    Map<String, Long> times;

    int maxNt;
    int procDim;
    double epsilon;
    double lbr;
    boolean rpFlag;

    public PCASkiingDROP(MacroBaseConf conf, int maxNt, double epsilon, double lbr, int b, int s){
        iter = 0;
        currNt = 0;
        pcaOpt = new PCASkiingOptimizer(epsilon, b, s);
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
        pcaOpt.extractData(records);
        log.debug("Extracted Records");
        pcaOpt.shuffleData();
        log.debug("Shuffled Data");
        pcaOpt.preprocess();
        log.debug("Processed Data");
        currNt = pcaOpt.getNextNt(iter, currNt, maxNt);
        log.debug("Beginning DROP");
        sw.start();
        do {
            log.debug("Iteration {}, {} samples", iter, currNt);
            pcaOpt.fit(currNt);
            currTransform = pcaOpt.getKCICached(iter, lbr); //function to get knee for K for this transform;
            currLBR = pcaOpt.LBRCI(currTransform, pcaOpt.getM(), 1.96);//paaOpt.LBRAttained(iter, currTransform); //TODO: this is repetitive. Refactor the getKI things to spit out
            pcaOpt.setLBRList(currNt, currLBR);
            pcaOpt.setTrainTimeList(currNt, (double) sw.elapsed(TimeUnit.MILLISECONDS));
            pcaOpt.setKList(currNt, currTransform.getColumnDimension());
            pcaOpt.setKDiff(iter, currTransform.getColumnDimension());
            log.debug("LOW {}, LBR {}, HIGH {}, VAR {} K {}.", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());
            currNt = pcaOpt.getNextNt(++iter, currNt, maxNt);
        } while (currNt < pcaOpt.getM() && currLBR[0] < lbr);

        log.debug("MIC DROP COMPLETE");
        finalTransform = currTransform.getData();

        log.debug("Computing Full Transform");
        pcaOpt.fit(pcaOpt.getM());
        currTransform = pcaOpt.getKFull(lbr);
        currLBR = pcaOpt.LBRCI(currTransform, pcaOpt.getM(), 1.96);//paaOpt.LBRAttained(iter, currTransform);
        log.debug("For full PCA, LOW {}, LBR {}, HIGH {}, VAR {} K {}", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());

        int i = 0;
        for (Datum d: records){
            RealVector transformedMetricVector = new ArrayRealVector(finalTransform[i++]);
            output.add(new Datum(d, transformedMetricVector));
        }
    }

    public Map<Integer, Double> genBasePlots(List<Datum> records){
        pcaOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", pcaOpt.getM(), pcaOpt.getN());
        pcaOpt.shuffleData();
        log.debug("Shuffled Data");
        pcaOpt.preprocess();
        log.debug("Processed Data");
        currNt = pcaOpt.getM();//pcaOpt.getNextNt(iter, currNt, maxNt);
        log.debug("Beginning PCA base run");
        pcaOpt.fit(currNt);
        //sw.start();
        //fftOpt.setTrainTimeList(currNt, (double) sw.elapsed(TimeUnit.MILLISECONDS));

        return pcaOpt.computeLBRs();
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }

    public Map<Integer, double[]> getLBR() { return pcaOpt.getLBRList();}

    public Map<Integer, Double> getTime(){
        return pcaOpt.getTrainTimeList();
    }

    public Map<Integer, Integer> getKList(){
        return pcaOpt.getKList();
    }

    public Map<Integer, Integer> getKItersList(){
        return pcaOpt.getKItersList();
    }

}
