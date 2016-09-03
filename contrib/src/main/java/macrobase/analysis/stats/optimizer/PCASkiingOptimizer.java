package macrobase.analysis.stats.optimizer;

import macrobase.analysis.stats.optimizer.util.PCA;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

public class PCASkiingOptimizer extends SkiingOptimizer {

    public PCASkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
    }

    @Override
    public void fit(int Nt) {
        RealMatrix trainMatrix = dataMatrix.getSubMatrix(0, Nt-1, 0, Nproc-1);
        this.pca = new PCA(trainMatrix);
    }

    @Override
    public RealMatrix transform(int K) {
        return this.pca.transform(dataMatrix, K);
    }

    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        RealMatrix currTransform = new Array2DRowRealMatrix();
        double[] LBR;
        for (int i = 5; i < Math.min(this.Nproc, this.NtList.get(iter)); i++){
            currTransform = this.transform(i);
            LBR = this.LBRAttained(iter, currTransform);
            if (LBR[1] >= targetLBR){
                break;
            }
        }
        return currTransform;
    }
}
