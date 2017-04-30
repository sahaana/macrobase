package macrobase.analysis.stats.optimizer;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.jtransforms.fft.DoubleFFT_1D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FFTSkiingOptimizer extends SkiingOptimizer {
    private static final Logger log = LoggerFactory.getLogger(FFTSkiingOptimizer.class);
    protected Map<Integer, Integer> KItersList;

    protected FastFourierTransformer transformer;

    protected DoubleFFT_1D t;

    protected RealMatrix paddedInput;
    protected double[][] td;

    RealMatrix transformedData;

    public FFTSkiingOptimizer(double epsilon) {
        super(epsilon);
        this.KItersList = new HashMap<>();
        transformer = new FastFourierTransformer(DftNormalization.STANDARD);
    }

    @Override
    public void fit(int Nt) {
        paddedInput = new Array2DRowRealMatrix(this.M, 2*N);
        paddedInput.setSubMatrix(this.dataMatrix.getData(), 0, 0);
        td = paddedInput.getData();
        t  = new DoubleFFT_1D(N);

        for (int i = 0; i < this.M; i++) {
            t.realForwardFull(td[i]);
        }
        transformedData = new Array2DRowRealMatrix(td);
    }

    @Override
    //K must be even.
    public RealMatrix transform(int K) {
        assert (K % 2 == 0);
        return transformedData.getSubMatrix(0,M-1,0,K-1);
    }



    public Map<Integer, Double> computeLBRs(){
        //confidence interval based method for getting K
        Map<Integer, Double> LBRs = new HashMap<>();
        double[] CI = {0,0,0};
        int interval = Math.max(2,this.N/11 + ((this.N/11) % 2)); //ensure even k always
        RealMatrix currTransform;
        for (int i = 26;(i <= this.N); i+= interval){
            currTransform = this.transform(i);
            CI = this.LBRCI(currTransform, M, 1.96, 2./N);
            log.debug("With K {}, LBR {} {} {}", i, CI[0], CI[1],CI[2]);
            LBRs.put(i, CI[1]);
        }
        return LBRs;
    }


    @Override
    public int getNextNt(int iter, int currNt) {
        return this.M;
    }

    public Map getKItersList(){ return KItersList; }

}
