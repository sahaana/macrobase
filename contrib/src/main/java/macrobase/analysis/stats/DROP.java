package macrobase.analysis.stats;

import Jama.Matrix;
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

public class DROP extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(DROP.class);
    private final MBStream<Datum> output = new MBStream<>();
    double epsilon;
    int iter;
    int currNt;
    int K;
    RealMatrix currTransform;
    PCAOptimizer pcaOpt;


    public DROP(MacroBaseConf conf){
        //for now, no options
        epsilon = 0.01;
        iter =  0;
        currNt = 0;
        pcaOpt = new PCAOptimizer(epsilon);
        K = 3;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        pcaOpt.extractData(records);
        currNt = pcaOpt.getNextNt(iter, K);
        ///currTransform is Null first iteration
        while (pcaOpt.epsilonAttained(iter, currTransform) > epsilon && currNt <= pcaOpt.getM() && iter < 50){
            log.debug("Iteration {} with {} samples ", iter, currNt);
            currTransform = pcaOpt.transform(K, currNt);
            currNt = pcaOpt.getNextNt(++iter, K);
            log.debug("LBR {}, next Nt {}", pcaOpt.epsilonAttained(iter, currTransform), currNt);
        }

        (new Matrix(currTransform.getData())).print(1,1);
        log.debug("Number of samples used {} to obtain LBR {}", pcaOpt.getNtList(iter-1), pcaOpt.epsilonAttained(iter-1, currTransform));
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
}
