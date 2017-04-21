package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PCASkiingOptimizer;
import macrobase.analysis.stats.optimizer.util.PCAPowerIteration;
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

    int maxNt;
    double epsilon;
    double lbr;

    PCAPowerIteration pwrIter;
    RealMatrix pwrEigs;
    RealMatrix transMatrix;
    RealMatrix testTransform;

    public PCASkiingDROP(MacroBaseConf conf, int maxNt, double epsilon, double lbr, int b, int s){
        iter = 0;
        currNt = 0;
        attainedLBR = false;
        pcaOpt = new PCASkiingOptimizer(epsilon, b, s);

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

    public void checkPwrIter(List<Datum> records){
        pcaOpt.extractData(records);
        log.debug("Extracted Records");
        pcaOpt.preprocess();
        log.debug("Processed Data");

        currNt = pcaOpt.getM(); //hardcoded for coffee.txt
        log.debug("Beginning DROP");

        pcaOpt.fit(currNt);
        pwrIter = new PCAPowerIteration(pcaOpt.getDataMatrix());

        transMatrix = this.pcaOpt.getTransformation();
        pwrEigs = pwrIter.computeEigs(40);

        for(int i = 0; i < pcaOpt.getN(); i++){
            for (int j = 0; j < 40; j++){
                if (Math.abs(transMatrix.getEntry(i,j)) - Math.abs(pwrEigs.getEntry(i,j)) > .001){
                    log.debug("{} {} {} {}", i, j, transMatrix.getEntry(i,j), pwrEigs.getEntry(i,j));
                }
            }
        }

        currTransform = this.pcaOpt.transform(30);
        testTransform =  pwrIter.transform(pcaOpt.getDataMatrix(),30);

        for(int i = 0; i < 56; i++){
            for (int j = 0; j < 30; j++){
                if (Math.abs(currTransform.getEntry(i,j)) - Math.abs(testTransform.getEntry(i,j)) > .001){
                   log.debug("{} {} {} {}", i, j, currTransform.getEntry(i,j), testTransform.getEntry(i,j));
                }
            }
        }
        testTransform = currTransform;
    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        pcaOpt.extractData(records);
        log.debug("Extracted Records");
        pcaOpt.shuffleData();
        log.debug("Shuffled Data");
        pcaOpt.preprocess();
        log.debug("Processed Data");
        currNt = pcaOpt.getNextNtPE(iter, currNt, maxNt, attainedLBR);
        log.debug("Beginning DROP");
        do {
            log.debug("Iteration {}, {} samples", iter, currNt);
            MD.reset();
            MD.start();
            pcaOpt.fit(currNt);
            currTransform = pcaOpt.getKCI(iter, lbr); //function to get knee for K for this transform;
            MD.stop();
            pcaOpt.updateMDRuntime(iter, currNt, (double) MD.elapsed(TimeUnit.MILLISECONDS));
            currLBR = pcaOpt.getCurrKCI();
            if (!attainedLBR) attainedLBR = (currLBR[0] >= lbr);

            pcaOpt.setLBRList(currNt, currLBR);
            pcaOpt.setKList(currNt, currTransform.getColumnDimension());
            pcaOpt.setKDiff(iter, currTransform.getColumnDimension());
            log.debug("LOW {}, LBR {}, HIGH {}, VAR {} K {}.", currLBR[0], currLBR[1], currLBR[2], currLBR[3], currTransform.getColumnDimension());
            //CurrNt, iter has been updated to next iterations
            currNt = pcaOpt.getNextNtPE(++iter, currNt, maxNt, attainedLBR);
            //pcaOpt.predictK(iter, currNt); //Decided to predict k here, after first k has been found
        } while (currNt < pcaOpt.getM());

        log.debug("MICDROP 'COMPLETE'");
        log.debug("Looked at {}/{} samples", pcaOpt.getNtList(iter-1), pcaOpt.getM());
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

    public Map<Integer, Integer> getKPred() { return pcaOpt.getKPredList(); }

    public Map<Integer, Double> getPredTime() { return pcaOpt.getPredictedTrainTimeList(); }

}
