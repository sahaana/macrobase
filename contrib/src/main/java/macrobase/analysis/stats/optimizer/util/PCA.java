package macrobase.analysis.stats.optimizer.util;

import Jama.Matrix;
import org.apache.commons.math3.linear.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PCA {
    private static final Logger log = LoggerFactory.getLogger(PCA.class);

    private RealMatrix dataMatrix; // A
    private RealMatrix centeredDataMatrix; // X
    private RealMatrix transformationMatrix; // V
    private RealVector columnMeans;
    private SingularValueDecomposition SVD; //gives X = UDV', U=mxp D=pxp V = pxn
    //private RealMatrix cachedTransform;
    private int N;
    private int M;
    private int P;

    public PCA(RealMatrix rawDataMatrix){
        this.dataMatrix = rawDataMatrix;
        this.M = rawDataMatrix.getRowDimension();
        this.N = rawDataMatrix.getColumnDimension();
        this.centeredDataMatrix = new Array2DRowRealMatrix(M,N);
        this.columnMeans = new ArrayRealVector(N);
        double sum;
        RealVector currVec;

        for (int i = 0; i < N; i++){
            currVec = this.dataMatrix.getColumnVector(i);
            sum = 0;
            for (double entry: currVec.toArray()){
                sum += entry;
            }
            columnMeans.setEntry(i, sum/M);
            currVec.mapSubtractToSelf(sum/M);
            centeredDataMatrix.setColumn(i, currVec.toArray());
        }

        SVD = new SingularValueDecomposition(centeredDataMatrix);
        this.transformationMatrix = SVD.getV();
        this.P = this.transformationMatrix.getRowDimension();
    }

    public int getN(){
        return this.N;
    }

    public int getM(){
        return this.M;
    }

    public RealMatrix transform(RealMatrix inputData, int K){
        if (K > Math.min(this.N,this.M)){
          log.warn("Watch your K...K {} M {} Nproc {}", K, this.M, this.N);
        }
        K = Math.min(Math.min(K, this.N), this.M);
        RealMatrix centeredInput = new Array2DRowRealMatrix(inputData.getData());
        RealMatrix transformation = this.transformationMatrix.getSubMatrix(0,this.P-1,0,K-1);
        RealVector currVec;
        for (int i = 0; i < this.N; i++){
            currVec = inputData.getColumnVector(i);
            currVec.mapSubtractToSelf(this.columnMeans.getEntry(i));
            centeredInput.setColumn(i, currVec.toArray());
        }
        return centeredInput.multiply(transformation);
    }

}
