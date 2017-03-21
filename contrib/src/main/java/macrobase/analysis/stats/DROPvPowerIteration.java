package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PCASkiingOptimizer;
import macrobase.analysis.stats.optimizer.util.PowerIteration;
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

public class DROPvPowerIteration extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(DROPvPowerIteration.class);

    private final MBStream<Datum> output;
    double[][] finalTransform;
    double[] currLBR;
    int currNt;
    int iter;

    RealMatrix currTransform;
    RealMatrix transformationMat;
    PCASkiingOptimizer pcaOpt;
    Stopwatch sw;

    PowerIteration pwrIter;
    RealMatrix pwrEigs;

    Map<String, Long> times;

    int maxNt;
    int procDim;
    double epsilon;
    double lbr;
    int b;
    int s;
    boolean rpFlag;

    public DROPvPowerIteration(MacroBaseConf conf, int maxNt, double epsilon, double lbr, int b, int s, boolean rpFlag){
        iter = 0;
        currNt = 0;
        pcaOpt = new PCASkiingOptimizer(epsilon, b, s);
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
        pcaOpt.extractData(records);
        log.debug("Extracted Records");
        pcaOpt.shuffleData();
        log.debug("Shuffled Data");
        pcaOpt.preprocess();
        log.debug("Processed Data");
        pwrIter = new PowerIteration(pcaOpt.getDataMatrix());
        //paaOpt.setKList(currNt,0); //hacky for test run of getNextNt
        //paaOpt.addNtList(0);
        currNt = pcaOpt.getNextNt(iter, currNt, maxNt);
        //currLBR = paaOpt.LBRAttained(iter, currTransform); //currTransform is currently null
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
            pwrEigs = pwrIter.computeEigs(currNt, pcaOpt.getN(), 10);//currTransform.getColumnDimension());
            //transformationMat = pcaOpt.getPca().getTransformationMatrix();
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
