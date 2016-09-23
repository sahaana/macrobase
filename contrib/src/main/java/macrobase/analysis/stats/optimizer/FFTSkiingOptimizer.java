package macrobase.analysis.stats.optimizer;

import macrobase.datamodel.Datum;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.eclipse.jetty.util.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class FFTSkiingOptimizer extends SkiingOptimizer {
    private static final Logger log = LoggerFactory.getLogger(PAASkiingOptimizer.class);
    protected Map<Integer, Integer> KItersList;

    //protected RealMatrix transformedData;
    protected FastFourierTransformer transformer;

    protected Complex[][] transformedData;
    protected int nextPowTwo;

    public FFTSkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
        this.KItersList = new HashMap<>();
        transformer = new FastFourierTransformer(DftNormalization.STANDARD);
    }

    @Override
    public void fit(int Nt) {
        //RealVector currVec;
        nextPowTwo = Math.max(2, 2 * Integer.highestOneBit(this.N - 1));

        transformedData = new Complex[this.M][nextPowTwo];//Array2DRowRealMatrix(this.M, nextPowTwo);
        RealMatrix paddedInput = new Array2DRowRealMatrix(this.M, nextPowTwo);
        paddedInput.setSubMatrix(this.dataMatrix.getData(), 0, 0);

        for (int i = 0; i < this.M; i++) {
            //Complex[] FFTOutput = transformer.transform(paddedInput.getRow(i), TransformType.FORWARD);
            //currVec = new ArrayRealVector();
            //for (Complex c : FFTOutput) {
              //  currVec = currVec.append(c.getReal());
                //currVec = currVec.append(c.getImaginary());
            //}
            transformedData[i] = transformer.transform(paddedInput.getRow(i), TransformType.FORWARD);
        }

    }

    public int[] complexArgSort(Complex[] in, boolean ascending){
        Integer[] indices = new Integer[in.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return (ascending ? 1 : -1) * Double.compare(in[o1].abs(), in[02].abs());
            }
        });
        return toPrim(indices);
    }

    public int[] argSort(int[] in, boolean ascending){
        Integer[] indices = new Integer[in.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return (ascending ? 1 : -1) * Integer.compare(in[o1], in[02]);
            }
        });
        return toPrim(indices);
    }

    public int[] toPrim(Integer[] in) {
        int[] out = new int[in.length];
        for (int i = 0; i < in.length; i++) {
            out[i] = in[i].intValue();
        }
        return out;
    }



    @Override
    public RealMatrix transform(int K) {
        assert (K % 2 == 0);

        RealMatrix output = new Array2DRowRealMatrix(this.M, K);
        int[] sortedIndices;
        Map<Integer, Integer> freqMap = new HashMap<>();
        int[] freqCounts = new int[this.nextPowTwo];
        int[] topFreqs;

        RealVector tempOut;
        Complex[] curr;

        for (int i = 0; i < this.M; i++) {
            sortedIndices = Arrays.copyOfRange(complexArgSort(transformedData[i], false), 0, K / 2);
            for (int j : sortedIndices) {
                freqCounts[j] += 1;
            }
        }
        topFreqs = Arrays.copyOfRange(argSort(freqCounts, false), 0, K / 2);

        for (int i = 0; i < this.M; i++){
            curr = transformedData[i];
            tempOut = new ArrayRealVector();
            for (int f: topFreqs){
                tempOut.append(curr[f].getReal());
                tempOut.append(curr[f].getImaginary());
            }
            output.setRowVector(i, tempOut);
        }
        return output;
    }

    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        return null;
    }
}
