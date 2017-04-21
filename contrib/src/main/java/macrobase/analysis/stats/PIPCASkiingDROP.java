package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PIPCASkiingOptimizer;
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

public class PIPCASkiingDROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(PIPCASkiingDROP.class);

    private final MBStream<Datum> output;
    double[][] finalTransform;
    double[] currLBR;
    int currNt;
    int iter;
    boolean attainedLBR;

    RealMatrix currTransform;
    PIPCASkiingOptimizer pipcaOpt;
    Stopwatch sw;
    Stopwatch MD;

    Map<String, Long> times;

    int maxNt;
    double epsilon;
    double lbr;

    public PIPCASkiingDROP(MacroBaseConf conf, int maxNt, double epsilon, double lbr, int b, int s){
        iter = 0;
        currNt = 0;
        attainedLBR = false;
        pipcaOpt = new PIPCASkiingOptimizer(epsilon, b, s);

        MD = Stopwatch.createUnstarted();

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
        pipcaOpt.extractData(records);
        log.debug("Extracted Records");
        pipcaOpt.shuffleData();
        log.debug("Shuffled Data");
        pipcaOpt.preprocess();
        log.debug("Processed Data");
        currNt = pipcaOpt.getNextNtPE(iter, currNt, maxNt, attainedLBR);
        log.debug("Beginning DROP");
        do {
            log.debug("Iteration {}, {} samples", iter, currNt);
            MD.reset();
            MD.start();
            pipcaOpt.fit(currNt);
            currTransform = pipcaOpt.getKCI(iter, lbr); //function to get knee for K for this transform;
            MD.stop();
            pipcaOpt.updateMDRuntime(iter, currNt, (double) MD.elapsed(TimeUnit.MILLISECONDS));
            currLBR = pipcaOpt.getCurrKCI();
            if (!attainedLBR) attainedLBR = (currLBR[0] >= lbr);

            pipcaOpt.setLBRList(currNt, currLBR);
            pipcaOpt.setKList(currNt, currTransform.getColumnDimension());
            pipcaOpt.setKDiff(iter, currTransform.getColumnDimension());
            log.debug("LOW {}, LBR {}, HIGH {}, VAR {} K {}.", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());
            //CurrNt, iter has been updated to next iterations
            currNt = pipcaOpt.getNextNtPE(++iter, currNt, maxNt, attainedLBR);
            //pipcaOpt.predictK(iter, currNt); //Moved inside get next Nt. Decided to predict k here, after first k has been found
        } while (currNt < pipcaOpt.getM());

        log.debug("MICDROP 'COMPLETE'");
        log.debug("Looked at {}/{} samples", pipcaOpt.getNtList(iter-1), pipcaOpt.getM());
        finalTransform = currTransform.getData();

        log.debug("Computing Full Transform");
        pipcaOpt.fit(pipcaOpt.getM());
        currTransform = pipcaOpt.getKFull(lbr);
        currLBR = pipcaOpt.LBRCI(currTransform, pipcaOpt.getM(), 1.96);//paaOpt.LBRAttained(iter, currTransform);
        log.debug("For full PCASVD, LOW {}, LBR {}, HIGH {}, VAR {} K {}", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());

        int i = 0;
        for (Datum d: records){
            RealVector transformedMetricVector = new ArrayRealVector(finalTransform[i++]);
            output.add(new Datum(d, transformedMetricVector));
        }
    }

    public Map<Integer, Double> genBasePlots(List<Datum> records){
        pipcaOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", pipcaOpt.getM(), pipcaOpt.getN());
        pipcaOpt.shuffleData();
        log.debug("Shuffled Data");
        pipcaOpt.preprocess();
        log.debug("Processed Data");
        currNt = pipcaOpt.getM();//pipcaOpt.getNextNt(iter, currNt, maxNt);
        log.debug("Beginning PCASVD base run");
        pipcaOpt.fit(currNt);
        //sw.start();
        //fftOpt.setTrainTimeList(currNt, (double) sw.elapsed(TimeUnit.MILLISECONDS));

        return pipcaOpt.computeLBRs();
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }

    public Map<Integer, double[]> getLBR() { return pipcaOpt.getLBRList();}

    public Map<Integer, Double> getTime(){
        return pipcaOpt.getTrainTimeList();
    }

    public Map<Integer, Integer> getKList(){
        return pipcaOpt.getKList();
    }

    public Map<Integer, Integer> getKItersList(){
        return pipcaOpt.getKItersList();
    }

    public Map<Integer, Integer> getKPred() { return pipcaOpt.getKPredList(); }

    public Map<Integer, Double> getPredTime() { return pipcaOpt.getPredictedTrainTimeList(); }

}
