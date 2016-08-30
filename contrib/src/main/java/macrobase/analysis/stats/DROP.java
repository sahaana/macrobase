package macrobase.analysis.stats;

import com.google.common.base.Stopwatch;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PCAOptimizer;
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

public class DROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(DROP.class);
    private final MBStream<Datum> output = new MBStream<>();
    double epsilon;
    double lbr;
    double currEp;
    double currLBR;
    int iter;
    int currNt;
    int K;
    int num_Nt;
    int processedDim;
    int b;
    int s;
    RealMatrix currTransform;
    PCAOptimizer pcaOpt;
    Stopwatch sw;


    public DROP(MacroBaseConf conf, int K, int num_Nt, int processedDim, double epsilon, double lbr){
        this.epsilon = epsilon;
        this.lbr = lbr;
        iter =  0;
        currNt = 0;
        pcaOpt = new PCAOptimizer(epsilon, lbr);
        currEp = 0;
        currLBR = 0;
        this.K = K;
        this.num_Nt = num_Nt;
        this.processedDim = processedDim;
        b = 300;
        s = 20;
        sw = Stopwatch.createUnstarted();
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        pcaOpt.extractData(records);
        log.debug("Extracted Records");
        pcaOpt.shuffleData();
        log.debug("Shuffled Data for randomized test, train");
        pcaOpt.preprocess(processedDim);
        log.debug("Processed data w/ PAA");
        currNt = pcaOpt.getNextNt(iter, K, num_Nt);
        //currEp = pcaOpt.LBRAttained(iter, currTransform);
        ///////currLBR = pcaOpt.LBRAttained(iter, epsilon, currTransform);
        currLBR = pcaOpt.blbLBRAttained(iter, epsilon, currTransform, b, s);
        log.debug("Beginning DROP");
        sw.start();
        ///currTransform is Null first iteration
        while (currLBR < lbr && currEp < epsilon && currNt <= pcaOpt.getM()){
            log.debug("Iteration {} with {} samples ", iter, currNt);
            //pcaOpt.printData(0,5,0,5);
            currTransform = pcaOpt.transform(K, currNt);
            //currEp = pcaOpt.LBRAttained(iter, epsilon, currTransform);
            ////////currLBR = pcaOpt.LBRAttained(iter, epsilon, currTransform);
            currLBR = pcaOpt.blbLBRAttained(iter, epsilon, currTransform, b, s);
            pcaOpt.setLBRList(pcaOpt.getNtList(iter), currLBR);
            pcaOpt.setTrainTimeList(pcaOpt.getNtList(iter), (double) sw.elapsed(TimeUnit.MILLISECONDS));
            log.debug("LBR {}", currLBR);
            currNt = pcaOpt.getNextNt(++iter, K, num_Nt);
        }

        log.debug("Number of samples used {} to obtain LBR {}", pcaOpt.getNtList(iter-1), pcaOpt.LBRAttained(iter-1, epsilon, currTransform));
        double[][] finalTransform = currTransform.getData();

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

    public Map<Integer, Double> getLBR(){
        return pcaOpt.getLBRList();
    }

    public Map<Integer, Double> getTime(){
        return pcaOpt.getTrainTimeList();
    }
}
