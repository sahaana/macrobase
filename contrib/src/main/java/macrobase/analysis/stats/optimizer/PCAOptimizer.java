package macrobase.analysis.stats.optimizer;

import Jama.Matrix;
import com.mkobos.pca_transform.PCA;
import macrobase.datamodel.Datum;

import java.util.List;
import java.util.Random;



public class PCAOptimizer extends Optimizer {
    public PCAOptimizer(double epsilon) {
        super(epsilon);
    }

    @Override
    public void extractData(List<Datum> records) {
        super.extractData(records);
    }

    @Override
    public Matrix tranform(int K, int Nt) {
        Matrix trainMatrix = this.dataMatrix.getMatrix(0, Nt, 0, this.N);
        PCA pca = new PCA(trainMatrix);
        return pca.transform(this.dataMatrix, PCA.TransformationType.WHITENING).getMatrix(0, this.M, 0, K);
    } //TODO: MAKE ONLY THIS PART IN JAMA, REST IN VECTOR/BLAS

    @Override
    public double epsilonAttained(Matrix transformedData) {
        int[] indicesA = new int[this.M];
        int[] indicesB = new int[this.M];
        int K = transformedData.getRowDimension();
        Random rand = new Random();

        for (int i = 0; i < this.M; i++){
            indicesA[i] = rand.nextInt(this.M);
            indicesB[i] = rand.nextInt(this.M);
        }

        Matrix tranformedDifferences = transformedData.getMatrix(indicesA,0,K).minus(transformedData.getMatrix(indicesB,0,K));
        Matrix originalDifferences = this.dataMatrix.getMatrix(indicesA,0,this.N).minus(this.dataMatrix.getMatrix(indicesB,0,this.N));


        return 0;
    }

    @Override
    public int getNextNt(int iter) {
        int interval = this.M /10;
        if (iter == 0){
            this.NtList.add(interval);
            return interval;
        }
        this.NtList.add(interval + this.NtList.get(iter-1));
        return this.NtList.get(iter);
    }
}
