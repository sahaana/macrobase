package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PCASkiingOptimizer;
import macrobase.analysis.stats.optimizer.util.PCASVD;
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

    RealMatrix transformedData;
    int currK;
    PCASkiingOptimizer pcaOpt;
    Stopwatch sw;
    Stopwatch MD;

    Map<String, Long> times;

    double qThresh;
    double lbr;

    PCASkiingOptimizer.PCAAlgo algo;
    PCASkiingOptimizer.work reuse;



    public PCASkiingDROP(MacroBaseConf conf, double qThresh, double lbr, int kExp, PCASkiingOptimizer.PCAAlgo algo, PCASkiingOptimizer.work reuse){
        iter = 0;
        currNt = 0;
        currK = 0;
        this.algo = algo;
        this.reuse = reuse;
        pcaOpt = new PCASkiingOptimizer(qThresh, kExp, algo, reuse);

        MD = Stopwatch.createUnstarted();
        sw = Stopwatch.createUnstarted();

        times = new HashMap<>();

        this.qThresh = qThresh;
        this.lbr = lbr;
        output = new MBStream<>();
    }

    public PCASkiingDROP(MacroBaseConf conf, double qThresh, double lbr, PCASkiingOptimizer.PCAAlgo algo){
        iter = 0;
        currNt = 0;
        currK = 0;
        this.algo = algo;
        pcaOpt = new PCASkiingOptimizer(qThresh, algo);

        MD = Stopwatch.createUnstarted();
        sw = Stopwatch.createUnstarted();

        times = new HashMap<>();

        this.qThresh = qThresh;
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
        pcaOpt.warmUp(currNt);
        log.debug("Warmed Up");
        log.debug("Beginning DROP");
        sw.start();
        do {
            ////log.debug("Iteration {}, {} samples", iter, currNt);
            MD.reset();
            MD.start();
            pcaOpt.fit(currNt);
            currK = pcaOpt.getKCI(iter, lbr); //function to get knee for K for this transform;
            MD.stop();
            //store how long (MD) this currNt took, diff between last and this, and store this as prevMD
            pcaOpt.updateMDRuntime(iter, currNt, (double) MD.elapsed(TimeUnit.MILLISECONDS));
            //returns the LBR CI from getKCI and then store it
            currLBR = pcaOpt.getCurrKCI();
            pcaOpt.setLBRList(currNt, currLBR);
            //store the K obtained and the diff in K from this currNt
            pcaOpt.setKList(currNt, currK);
            pcaOpt.setKDiff(iter, currK);
            ////log.debug("LOW {}, LBR {}, HIGH {}, VAR {} K {}.", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currK);
            //CurrNt, iter has been updated to next iterations. Pass in next iter (so ++iter) to this function
            currNt = pcaOpt.getNextNtPE(++iter, currNt);
        } while (currNt < pcaOpt.getM());

        sw.stop();
        transformedData = pcaOpt.transform(currK);

        log.debug("MICDROP 'COMPLETE'");
        log.debug("Looked at {}/{} samples", pcaOpt.getNtList(iter-1), pcaOpt.getM());
        finalTransform = transformedData.getData();

        /*
        log.debug("Computing Full Transform");
        pcaOpt = new PCASkiingOptimizer(qThresh, PCASkiingOptimizer.PCAAlgo.SVD, );
        pcaOpt.extractData(records);
        pcaOpt.preprocess();
        currNt = pcaOpt.getNextNtFull(0,currNt);
        log.debug("Running SVD");
        pcaOpt.fit(currNt);
        transformedData = pcaOpt.getKFull(lbr);
        currLBR = pcaOpt.LBRCI(transformedData, pcaOpt.getM(), 1.96);
        log.debug("For full PCASVD, LOW {}, LBR {}, HIGH {}, VAR {} K {}", currLBR[0], currLBR[1], currLBR[2], currLBR[3], transformedData.getColumnDimension());
        */

        int i = 0;
        for (Datum d: records){
            RealVector transformedMetricVector = new ArrayRealVector(finalTransform[i++]);
            output.add(new Datum(d, transformedMetricVector));
        }
    }

    public Map<String,Map<Integer, Double>> genBasePlots(List<Datum> records){
        pcaOpt = new PCASkiingOptimizer(qThresh, algo);
        pcaOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", pcaOpt.getM(), pcaOpt.getN());
        pcaOpt.preprocess();
        log.debug("Processed Data");

        log.debug("Beginning PCASVD base run");
        return pcaOpt.computeLBRs();
    }

    public double[] getDataSpectrum(List<Datum> records){
        pcaOpt = new PCASkiingOptimizer(qThresh, PCASkiingOptimizer.PCAAlgo.SVD);
        pcaOpt.extractData(records);
        log.debug("Extracted {} Records of len {}", pcaOpt.getM(), pcaOpt.getN());
        PCASVD svd = new PCASVD(pcaOpt.getDataMatrix());
        log.debug("Dumping Spectrum");
        return svd.getSpectrum();
    }



    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }

    public long totalTime() { return sw.elapsed(TimeUnit.MILLISECONDS);}

    public int finalK() { return currK; }

    public Map<Integer, double[]> getLBR() { return pcaOpt.getLBRList();}

    public Map<Integer, Double> getTime(){
        return pcaOpt.getTrainTimeList();
    }

    public Map<Integer, Integer> getKList(){
        return pcaOpt.getKList();
    }

    public Map<Integer, double[]> getMDRuntimes() { return pcaOpt.bundleMDTimeGuess(); }

    public Map<Integer, Double> getTrueObjective() { return pcaOpt.getTrueObjective(); }

    public Map<Integer, Double> getPredictedObjective() { return pcaOpt.getPredictedObjective(); }

    public Map<Integer, Integer> getKItersList(){
        return pcaOpt.getKItersList();
    }

    public Map<Integer, Integer> getKPred() { return pcaOpt.getKPredList(); }

    public Map<Integer, Double> getPredTime() { return pcaOpt.getPredictedTrainTimeList(); }

}
