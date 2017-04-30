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
    double[][] finalTransform;
    double[] currLBR;
    int currNt;
    int iter;
    boolean attainedLBR;

    RealMatrix currTransform;
    PCASkiingOptimizer pcaOpt;
    Stopwatch sw;
    Stopwatch MD;

    Map<String, Long> times;

    double epsilon;
    double lbr;

    PCASkiingOptimizer.PCAAlgo algo;



    public PCASkiingDROP(MacroBaseConf conf, double epsilon, double lbr, PCASkiingOptimizer.PCAAlgo algo){
        iter = 0;
        currNt = 0;
        attainedLBR = false;
        this.algo = algo;
        pcaOpt = new PCASkiingOptimizer(epsilon, algo);

        MD = Stopwatch.createUnstarted();

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
        pcaOpt.extractData(records);
        log.debug("Extracted {} Records of dim {}", pcaOpt.getM(),pcaOpt.getN());
        pcaOpt.preprocess();
        log.debug("Processed Data");
        currNt = pcaOpt.getNextNtPE(iter, currNt);
        log.debug("Beginning DROP");
        do {
            log.debug("Iteration {}, {} samples", iter, currNt);
            MD.reset();
            MD.start();
            pcaOpt.fit(currNt);
            currTransform = pcaOpt.getKCI(iter, lbr); //function to get knee for K for this transform;
            MD.stop();
            //store how long (MD) this currNt took, diff between last and this, and store this as prevMD
            pcaOpt.updateMDRuntime(iter, currNt, (double) MD.elapsed(TimeUnit.MILLISECONDS));
            //returns the LBR CI from getKCI and then store it
            currLBR = pcaOpt.getCurrKCI();
            pcaOpt.setLBRList(currNt, currLBR);
            //store the K obtained and the diff in K from this currNt
            pcaOpt.setKList(currNt, currTransform.getColumnDimension());
            pcaOpt.setKDiff(iter, currTransform.getColumnDimension());
            log.debug("LOW {}, LBR {}, HIGH {}, VAR {} K {}.", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());
            //CurrNt, iter has been updated to next iterations. Pass in next iter (so ++iter) to this function
            currNt = pcaOpt.getNextNtPE(++iter, currNt);
        } while (currNt < pcaOpt.getM());

        log.debug("MICDROP 'COMPLETE'");
        log.debug("Looked at {}/{} samples", pcaOpt.getNtList(iter-1), pcaOpt.getM());
        finalTransform = currTransform.getData();

        log.debug("Computing Full Transform");
        pcaOpt = new PCASkiingOptimizer(epsilon, PCASkiingOptimizer.PCAAlgo.SVD);
        pcaOpt.extractData(records);
        pcaOpt.shuffleData();
        pcaOpt.preprocess();
        currNt = pcaOpt.getNextNtFull(0,currNt);
        log.debug("Running SVD");
        pcaOpt.fit(currNt);
        currTransform = pcaOpt.getKFull(lbr);
        currLBR = pcaOpt.LBRCI(currTransform, pcaOpt.getM(), 1.96);
        log.debug("For full PCASVD, LOW {}, LBR {}, HIGH {}, VAR {} K {}", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());

        int i = 0;
        for (Datum d: records){
            RealVector transformedMetricVector = new ArrayRealVector(finalTransform[i++]);
            output.add(new Datum(d, transformedMetricVector));
        }
    }

    public Map<Integer, Double> genBasePlots(List<Datum> records){
        pcaOpt = new PCASkiingOptimizer(epsilon, algo);
        pcaOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", pcaOpt.getM(), pcaOpt.getN());
        pcaOpt.preprocess();
        log.debug("Processed Data");
        currNt = pcaOpt.getM();
        log.debug("Beginning PCASVD base run");
        pcaOpt.fit(currNt);
        //sw.start();

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

    public Map<Integer, Integer> getKPred() { return pcaOpt.getKPredList(); }

    public Map<Integer, Double> getPredTime() { return pcaOpt.getPredictedTrainTimeList(); }

}
