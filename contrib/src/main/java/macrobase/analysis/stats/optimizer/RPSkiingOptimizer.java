package macrobase.analysis.stats.optimizer;

import com.google.common.base.Stopwatch;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.Matrices;
import no.uib.cipr.matrix.QR;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class RPSkiingOptimizer extends SkiingOptimizer{
    private static final Logger log = LoggerFactory.getLogger(RPSkiingOptimizer.class);
    protected Map<Integer, Integer> KItersList;
    protected List<Integer> factors;
    protected RealMatrix largestTransformation;
    protected int largestK;

    public RPSkiingOptimizer(double qThresh){
        super(qThresh);
        this.KItersList = new HashMap<>();
        this.largestK = 0;
    }

    @Override
    public void fit(int Nt) {
    }

    @Override
    public void preprocess() {
        super.preprocess();
    }

    @Override
    public RealMatrix transform(int K) {
        if (K > largestK){
            cacheTransform(K);
        }
       return largestTransformation.getSubMatrix(0,M-1,0,K-1);
    }

    public void cacheTransform(int K) {
        if (K >= largestK){
            largestK = K;
        }
        Random rand = new Random();
        DenseMatrix omega = new DenseMatrix(N,K);
        DenseMatrix in = new DenseMatrix(dataMatrix.getData());
        DenseMatrix out = new DenseMatrix(M, K);

        //generate gaussian random matrix
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < K; j++)
                omega.set(i, j, rand.nextGaussian());
        }
        /* can ortho to be really good, but this is expensive. Asymptotically as much as PCAFast
        QR qr = new QR(N, K);
        qr.factor(omega);
        omega = qr.getQ();
        */
        in.mult(omega, out);
        largestTransformation = new Array2DRowRealMatrix(Matrices.getArray(out));
    }

    public Map<String,Map<Integer, Double>> computeLBRs(){
        //confidence interval based method for getting K. Just doing same k as FFT
        Map<Integer, Double> LBRs = new HashMap<>();
        Map<Integer, Double> times = new HashMap<>();
        Map<String, Map<Integer, Double>> results = new HashMap<>();

        Stopwatch sw =  Stopwatch.createUnstarted();

        sw.start();
        this.fit(M);
        int max = N;
        this.cacheTransform(max);

        sw.stop();
        times.put(0, (double) sw.elapsed(TimeUnit.MILLISECONDS));

        double[] CI;
        int interval = Math.max(2,this.N/20 + ((this.N/20) % 2)); //ensure even k always
        RealMatrix currTransform;
        for (int i = 2;i <= max; i+= interval){
            sw.reset();
            sw.start();
            currTransform = this.transform(i);
            sw.stop();

            CI = this.LBRCI(currTransform, M, qThresh, ((double) N)/i);
            log.debug("With K {}, LBR {} {} {}", i, CI[0], CI[1],CI[2]);
            LBRs.put(i, CI[1]);
            times.put(i, (double) sw.elapsed(TimeUnit.MILLISECONDS));
        }
        results.put("LBR", LBRs);
        results.put("time", times);
        return results;
    }

    public Map getKItersList(){ return KItersList; }
}
