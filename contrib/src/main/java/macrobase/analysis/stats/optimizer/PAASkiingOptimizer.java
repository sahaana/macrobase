package macrobase.analysis.stats.optimizer;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class PAASkiingOptimizer extends SkiingOptimizer{

    public PAASkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
    }

    @Override
    public void fit(int Nt) {

    }

    @Override
    public RealMatrix transform(int K) {
        // Implementation of PAA. TODO: not optimized as Keogh says
        assert (this.N % K == 0 );
        RealMatrix output = new Array2DRowRealMatrix(this.M, K);
        RealVector currVec;
        double temp;
        int entriesAveraged = this.N / K;
        //this.dataMatrix = new Array2DRowRealMatrix(this.M, K);
        //this.Nproc = K;

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
        return new Array2DRowRealMatrix();
    }
}
