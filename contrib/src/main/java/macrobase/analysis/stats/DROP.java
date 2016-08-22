package macrobase.analysis.stats;

import Jama.Matrix;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.optimizer.PCAOptimizer;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;


import java.util.List;

public class DROP extends FeatureTransform {
    private final MBStream<Datum> output = new MBStream<>();
    double epsilon;
    int iter;
    int currNt;
    int K = 10;
    Matrix currTranform;
    PCAOptimizer pcaOpt;


    public DROP(MacroBaseConf conf){
        //for now, no options
        epsilon = 0.01;
        iter =  0;
        currNt = 0;
        pcaOpt = new PCAOptimizer(epsilon);
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        pcaOpt.extractData(records);
        currNt = pcaOpt.getNextNt(iter++);
        while (pcaOpt.epsilonAttained(iter, currTranform) > epsilon && currNt < pcaOpt.getM()){
            currNt = pcaOpt.getNextNt(iter++);
            currTranform = pcaOpt.transform(K,currNt);
        }

        assert (currTranform.getRowDimension() == K);

        double[][] finalTransform = currTranform.getArray();
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
