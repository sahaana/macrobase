package macrobase.analysis.stats.optimizer;

import Jama.Matrix;
import com.mkobos.pca_transform.PCA;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.core.matrix.DoubleVector;

import java.util.List;
import java.util.Random;




public class PCAOptimizer extends Optimizer {

    private static final Logger log = LoggerFactory.getLogger(PCAOptimizer.class);

    public PCAOptimizer(double epsilon) {
        super(epsilon);
    }
    @Override
    public Matrix transform(int K, int Nt) {
        Matrix trainMatrix = this.dataMatrix.getMatrix(0, Nt, 0, 3);
        PCA pca = new PCA(trainMatrix);
        return pca.transform(this.dataMatrix, PCA.TransformationType.WHITENING).getMatrix(0, this.M-1, 0, K-1);
    } //TODO: MAKE ONLY THIS PART IN JAMA, REST IN VECTOR/BLAS

    @Override
    public double epsilonAttained(int iter, Matrix transformedData) {
        if (iter == 0){
            return 1;
        }

        DoubleVector transformedDists;
        DoubleVector trueDists;
        double lbr;
        int[] indicesA = new int[this.M];
        int[] indicesB = new int[this.M];
        int K = transformedData.getColumnDimension();
        Random rand = new Random();

        for (int i = 0; i < this.M; i++){
            indicesA[i] = rand.nextInt(this.M);
            indicesB[i] = rand.nextInt(this.M);
        }

        log.debug("transformed data dim {} x {}",transformedData.getRowDimension(),transformedData.getColumnDimension());
        log.debug("K {}", K);

        transformedDists = this.calcDistances(transformedData.getMatrix(indicesA,0,K-1), transformedData.getMatrix(indicesB,0,K-1));
        trueDists = this.calcDistances(this.dataMatrix.getMatrix(indicesA,0,this.N-1), this.dataMatrix.getMatrix(indicesB,0,this.N-1));
        lbr = this.LBR(trueDists, transformedDists);

        return lbr;
    }

    @Override
    public int getNextNt(int iter) {
        int interval = this.M / 2;
        if (iter == 0){
            this.NtList.add(interval);
            return interval;
        }
        this.NtList.add(interval + this.NtList.get(iter-1));
        return this.NtList.get(iter);
    }
}
