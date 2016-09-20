package macrobase.analysis.stats.optimizer;

import macrobase.datamodel.Datum;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

public class FFTSkiingOptimizer extends SkiingOptimizer {

    public FFTSkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
    }

    @Override
    public void fit(int Nt) {
        int nextPowTwo = Math.max(2,2*Integer.highestOneBit(this.N-1));
        RealMatrix output = new Array2DRowRealMatrix(this.M, nextPowTwo);
        RealMatrix paddedInput = new Array2DRowRealMatrix(this.M, nextPowTwo);

        paddedInput.setSubMatrix(this.dataMatrix.getData(), 0, 0);
        /*



        for (Datum d: records){
            metricVector = d.metrics();
            // TODO: look for decent FFT implementation that doesn't need pwr of 2
            nextPowTwo = Math.max(2,2*Integer.highestOneBit(metricVector.getDimension()-1));
            paddedInput = metricVector.append(new ArrayRealVector(nextPowTwo - metricVector.getDimension()));

            transformer = new FastFourierTransformer(DftNormalization.STANDARD);
            FFTOutput = transformer.transform(paddedInput.toArray(), TransformType.FORWARD);
            transformedMetricVector = new ArrayRealVector();
            for (Complex c: FFTOutput){
                transformedMetricVector = transformedMetricVector.append(c.getReal());
                transformedMetricVector = transformedMetricVector.append(c.getImaginary());
            }
            output.add(new Datum(d, transformedMetricVector));*/
        }

    @Override
    public RealMatrix transform(int K) {
        return null;
    }

    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        return null;
    }
}
