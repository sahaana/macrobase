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

    public PAASkiingOptimizer(double epsilon){
        super(epsilon);
        this.KItersList = new HashMap<>();
    }

    @Override
    public void fit(int Nt) {
    }

    @Override
    public void preprocess() {
        super.preprocess();
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


    private List<Integer> findFactors(){
        List<Integer> factors = new ArrayList<>();
        for (int i = 1; i <= this.N; i++) {
            if (this.N % i == 0) factors.add(i);
        }
        return factors;
    }

    public Map<Integer, Double> computeLBRs(){
        Map<Integer, Double> LBRs = new HashMap<>();
        double[] CI;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();
        for (int i: factors){
            currTransform = this.transform(i);
            CI = this.LBRCI(currTransform,M, 1.96, ((double) N) /i);
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
