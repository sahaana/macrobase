package macrobase.analysis.stats.optimizer;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PAASkiingOptimizer extends SkiingOptimizer{
    private static final Logger log = LoggerFactory.getLogger(PAASkiingOptimizer.class);
    protected Map<Integer, Integer> KItersList;
    protected List<Integer> factors;

    public PAASkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
        this.KItersList = new HashMap<>();
    }

    @Override
    public void fit(int Nt) {
    }

    @Override
    public void preprocess(int reducedDim) {
        super.preprocess(reducedDim);
        this.factors = this.findFactors();
    }

    @Override
    public RealMatrix transform(int K) {
        // Implementation of PAA. TODO: not optimized as Keogh says
        assert (this.N % K == 0 );
        RealMatrix output = new Array2DRowRealMatrix(this.M, K);
        RealVector currVec;
        double temp;
        int entriesAveraged = this.N / K;

        for (int i = 0; i < this.M; i++){
            currVec = this.dataMatrix.getRowVector(i);
            temp = 0;
            for (int j = 0; j < this.N; j++){
                if (j % entriesAveraged == 0 && j != 0){
                    output.setEntry(i, j/entriesAveraged - 1,temp/entriesAveraged);
                    temp = 0;
                }
                temp += currVec.getEntry(j);
                if (j == this.N - 1){
                    output.setEntry(i,this.N/entriesAveraged - 1, temp/entriesAveraged);
                }
            }
        }
        return output;
    }

    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        double LBR;
        double[] CI;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();
        for (int i: factors){
            currTransform = this.transform(i);
            this.Nproc = i;
            LBR = evalK(targetLBR, currTransform);
            CI = this.LBRCI(currTransform,M, 1.96);
            log.debug("With K {}, LBR {} {} {}", i, CI[0], CI[1],CI[2]);
            //System.out.println(String.format("With K {}, LBR {}", i, LBR));
            if (targetLBR < LBR) {
                this.feasible = true;
                this.lastFeasible = i;
                return currTransform;
            }
        }
        this.Nproc = this.N;
        return this.transform(this.N);
    }

    public RealMatrix getKCI(int iter, double targetLBR) {
        //same as getK, but with binary search over factor list instead
        double LBR;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();

        int iters = 0;
        int low = 0;
        int high = factors.size();
        if (this.feasible) high = this.lastFeasible;
        int mid = (low + high) / 2;

        while (low < high) {
            currTransform = this.transform(factors.get(mid));
            this.Nproc = factors.get(mid);
            LBR = evalK(targetLBR, currTransform);//this.LBRCI(currTransform, numPairs, thresh)[0];
            if (targetLBR < LBR) {
                currTransform = this.transform(factors.get(mid - 1));
                this.Nproc = factors.get(mid-1);
                LBR = evalK(targetLBR, currTransform);//this.LBRCI(currTransform, numPairs, thresh)[0];
                if (targetLBR > LBR) {
                    this.feasible = true;
                    this.lastFeasible = factors.get(mid);
                    KItersList.put(this.NtList.get(iter), iters);
                    this.Nproc = factors.get(mid);
                    return this.transform(factors.get(mid));
                }
                high = mid - 1;
            } else if (targetLBR > LBR) {
                low = mid + 1;
            } else {
                high = mid;
            }
            iters += 1;
            mid = (low + high) / 2;
        }
        this.feasible = true;
        this.lastFeasible = factors.get(mid);
        KItersList.put(this.NtList.get(iter), iters);
        this.Nproc = factors.get(mid);
        return this.transform(factors.get(mid));
    }



    private double evalK(double LBRThresh, RealMatrix currTransform){
        double[] CI;
        double q = 1.96;
        double prevMean = 0;
        int numPairs = (this.M)*((this.M) - 1)/2;
        int currPairs = 100;//Math.max(5, this.M);//new Double(0.005*numPairs).intValue());
        while (currPairs < numPairs){
            CI = this.LBRCI(currTransform,currPairs, q);
            if (CI[0] > LBRThresh){
                return LBRThresh;
            }
            else if (CI[2] < LBRThresh){
                return 0.0;
            }
            else if (Math.abs(CI[1]-prevMean) < .02){
                return 0.0;
            }
            else {
                currPairs *= 2;
                prevMean = CI[1];
            }
        }
        return 0.0;
    }

    private List<Integer> findFactors(){
        List<Integer> factors = new ArrayList<>();
        for (int i = 1; i < this.Nproc; i++) {
            if (Nproc % i == 0) factors.add(i);
        }
        return factors;
    }

    @Override
    public int getNextNt(int iter, int currNt, int maxNt) {
        return this.M;
    }

    public Map getKItersList(){ return KItersList; }
}
