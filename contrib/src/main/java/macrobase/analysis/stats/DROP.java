package macrobase.analysis.stats;

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
    double currEp;
    int iter;
    int currNt;
    int K;
    int num_Nt;
    int processedDim;
    RealMatrix currTransform;
    PCAOptimizer pcaOpt;


    public DROP(MacroBaseConf conf, int K, int num_Nt, int processedDim){
        //for now, no options
        epsilon = 50;//0.01; checking for broked-
        iter =  0;
        currNt = 0;
        pcaOpt = new PCAOptimizer(epsilon);
        currEp = 0;
        this.K = K;
        this.num_Nt = num_Nt;
        this.processedDim = processedDim;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        pcaOpt.extractData(records);
        pcaOpt.preprocess(processedDim);
        //pcaOpt.printData(0,pcaOpt.getM()/10,0,pcaOpt.getNproc()/10);
        currNt = pcaOpt.getNextNt(iter, K, num_Nt);
        currEp = pcaOpt.epsilonAttained(iter, currTransform);
        ///currTransform is Null first iteration
        while (currEp < epsilon && currNt <= pcaOpt.getM() && iter < 50){
            log.debug("Iteration {} with {} samples ", iter, currNt);
            //pcaOpt.printData(0,5,0,5);
            currTransform = pcaOpt.transform(K, currNt);
            //pcaOpt.printData(0,5,0,5);
            currNt = pcaOpt.getNextNt(++iter, K, num_Nt);
            currEp = pcaOpt.epsilonAttained(iter, currTransform);
            log.debug("LBR {}, next Nt {}", currEp, currNt);
        }

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
