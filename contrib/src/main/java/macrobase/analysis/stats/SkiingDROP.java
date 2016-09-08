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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SkiingDROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(SkiingDROP.class);

    private final MBStream<Datum> output;
    double[][] finalTransform;
    double[] currLBR;
    int currNt;
    int iter;

    RealMatrix currTransform;
    PCASkiingOptimizer pcaOpt;
    Stopwatch sw;

    int maxNt;
    int procDim;
    double epsilon;
    double lbr;
    int b;
    int s;

    public SkiingDROP(MacroBaseConf conf, int maxNt, double epsilon, double lbr, int b, int s){
        iter = 0;
        currNt = 0;
        pcaOpt = new PCASkiingOptimizer(epsilon, b, s);
        sw = Stopwatch.createUnstarted();

        this.maxNt = maxNt;
        this.procDim = 707; //This is an appendix
        this.epsilon = epsilon;
        this.lbr = lbr;
        this.b = b;
        this.s = s;

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
        pcaOpt.preprocess(procDim);
        log.debug("Processed Data");
        //pcaOpt.setKList(currNt,0); //hacky for test run of getNextNt
        //pcaOpt.addNtList(0);
        currNt = pcaOpt.getNextNt(iter, currNt, maxNt);
        //currLBR = pcaOpt.LBRAttained(iter, currTransform); //currTransform is currently null
        log.debug("Beginning DROP");
        sw.start();
        do {
            log.debug("Iteration {}, {} samples", iter, currNt);
            pcaOpt.fit(currNt);
            currTransform = pcaOpt.getK(iter, lbr); //function to get knee for K for this transform;
            currLBR = pcaOpt.LBRAttained(iter, currTransform);
            pcaOpt.setLBRList(currNt, currLBR);
            pcaOpt.setTrainTimeList(currNt, (double) sw.elapsed(TimeUnit.MILLISECONDS));
            pcaOpt.setKList(currNt, currTransform.getColumnDimension());
            pcaOpt.setKDiff(iter, currTransform.getColumnDimension());
            log.debug("LOW {}, LBR {}, HIGH {}, K {}", currLBR[0], currLBR[1], currLBR[2], currTransform.getColumnDimension());
            currNt = pcaOpt.getNextNt(++iter, currNt, maxNt);
        } while (currNt < pcaOpt.getM() && currLBR[0] < lbr);

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

    public Map<Integer, double[]> getLBR() { return pcaOpt.getLBRList();}

    public Map<Integer, Double> getTime(){
        return pcaOpt.getTrainTimeList();
    }

    public Map<Integer, Integer> getKList(){
        return pcaOpt.getKList();
    }

}
