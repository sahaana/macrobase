package macrobase.analysis.stats.optimizer;

import Jama.Matrix;
import macrobase.analysis.stats.optimizer.util.PCA;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;




public class PCAOptimizer extends Optimizer {

    private static final Logger log = LoggerFactory.getLogger(PCAOptimizer.class);

    public PCAOptimizer(double epsilon) {
        super(epsilon);
    }
    @Override
    public RealMatrix transform(int K, int Nt) {
        //log.debug("Matrix Dims {} {}", this.dataMatrix.getRowDimension(), this.dataMatrix.getColumnDimension());
        //log.debug("Trying to access {} {}", Nt, this.N -1);
        RealMatrix trainMatrix = this.dataMatrix.getSubMatrix(0, Nt-1, 0, this.N - 1);
        PCA pca = new PCA(trainMatrix);


        //log.debug("Trained on matrix accessing {} {}", Nt-1, this.N-1);
        /*
        log.debug("Trained on matrix:");
        (new Matrix(trainMatrix.getData())).print(8,5);

        log.debug("Transforming:");
        (new Matrix(this.dataMatrix.getData())).print(8,5);

        log.debug("Tranformed into:");
        (new Matrix(pca.transform(this.dataMatrix, K).getData())).print(8,5);
        //log.debug("Transformed matrix Dims {} {}", pca.transform(this.dataMatrix, K).getRowDimension(), pca.transform(this.dataMatrix, K).getColumnDimension());
        //log.debug("Trying to access {} {}", this.M-1, K -1); */
        return pca.transform(this.dataMatrix, K);
    }

    @Override
    public double epsilonAttained(int iter, RealMatrix transformedData) {
        if (iter == 0){
            return 1;
        }

        RealVector transformedDists;
        RealVector trueDists;
        double lbr;
        int K = transformedData.getColumnDimension();
        int[] indicesA = new int[this.M];
        int[] indicesB = new int[this.M];
        int[] allIndices = new int[N];
        Random rand = new Random();

        for (int i = 0; i < this.N; i++){
            allIndices[i] = i; //TODO: this is stupid
        }
        int[] kIndices = Arrays.copyOf(allIndices,K);

        for (int i = 0; i < this.M; i++){
            indicesA[i] = rand.nextInt(this.M);
            indicesB[i] = rand.nextInt(this.M);
            while(indicesA[i] == indicesB[i]){
                indicesA[i] = rand.nextInt(this.M);
            }
        }


        //log.debug("transformed data dim {} x {}",transformedData.getRowDimension(),transformedData.getColumnDimension());
        //log.debug("K {}", K);

        transformedDists = this.calcDistances(transformedData.getSubMatrix(indicesA,kIndices), transformedData.getSubMatrix(indicesB, kIndices)).mapMultiply(Math.sqrt(this.N/K));
        trueDists = this.calcDistances(this.dataMatrix.getSubMatrix(indicesA,allIndices), this.dataMatrix.getSubMatrix(indicesB,allIndices));
        lbr = this.LBR(trueDists, transformedDists);
        //System.out.println(lbr);
        return lbr;
    }

    @Override
    public int getNextNt(int iter, int K) {
        int interval = 1;//this.M/ 1;
        if (iter == 0){
            this.NtList.add(Math.max(K,interval)); //this is to make sure we have at least K samples for PCA
            return Math.max(K,interval);
        }
        this.NtList.add(interval + this.NtList.get(iter-1));
        return this.NtList.get(iter);
    }
}

