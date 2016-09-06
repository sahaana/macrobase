package macrobase.analysis.stats.optimizer;

import macrobase.analysis.stats.optimizer.util.PCA;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.*;

public class PCASkiingOptimizer extends SkiingOptimizer {

    public PCASkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
    }

    @Override
    public void fit(int Nt) {
        RealMatrix trainMatrix = dataMatrix.getSubMatrix(0, Nt-1, 0, Nproc-1);
        this.pca = new PCA(trainMatrix);
    }

    public void maryFit(int Nt){
        RealVector mean = new ArrayRealVector(Nproc);
        RealVector covV = new ArrayRealVector(Nproc, 1d/Nproc);
        RealMatrix covM = new DiagonalMatrix(covV.toArray());
        MultivariateNormalDistribution  mnd = new MultivariateNormalDistribution(mean.toArray(), covM.getData());
        //mnd.reseedRandomGenerator(randomSeed);
        RealMatrix randomProjectionMatrix = new Array2DRowRealMatrix(mnd.sample(Nt));
        RealMatrix trainMatrix = dataMatrix.multiply(randomProjectionMatrix);
        this.pca = new PCA(trainMatrix);
    }

    @Override
    public RealMatrix transform(int K) {
        return this.pca.transform(dataMatrix, K);
    }


    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        int mid;
        double LBR;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();

        int low = 0;
        int high = Math.min(this.Nproc, this.NtList.get(iter)) - 1;

        while (low <= high) {
            mid = (low + high) / 2;
            currTransform = this.transform(mid);
            LBR = this.meanLBR(iter, currTransform);
            if (targetLBR < LBR) {
                currTransform = this.transform(mid - 1);
                LBR = this.meanLBR(iter, currTransform);
                if (targetLBR > LBR) {
                    return currTransform;
                }
                high = mid - 1;
            } else if (targetLBR > LBR) {
                low = mid + 1;
            } else {
                return currTransform;
            }
        }
        return this.transform(Math.min(this.Nproc, this.NtList.get(iter)) - 1);
    }
        /*
        RealMatrix currTransform = new Array2DRowRealMatrix();
        double LBR;
        for (int i = 5; i < Math.min(this.Nproc, this.NtList.get(iter)); i++){
            currTransform = this.transform(i);
            LBR = this.meanLBR(iter, currTransform);
            if (LBR >= targetLBR){
                System.out.println(LBR);
                break;
            }
        }
        return currTransform;
    } */
}
