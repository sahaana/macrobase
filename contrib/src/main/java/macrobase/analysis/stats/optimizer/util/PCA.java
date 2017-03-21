package macrobase.analysis.stats.optimizer.util;

import no.uib.cipr.matrix.*;
import org.apache.commons.math3.linear.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PCA {
    private static final Logger log = LoggerFactory.getLogger(PCA.class);

    private RealMatrix dataMatrix; // A
    private RealMatrix centeredDataMatrix; // X
    private RealMatrix transformationMatrix; // V
    private RealVector columnMeans;
    private SVD svd; //gives X = UDV', U=mxp D=pxp V = pxn
    private int N;
    private int M;
    private int P;

    public PCA(RealMatrix rawDataMatrix) {
        this.dataMatrix = rawDataMatrix;
        this.M = rawDataMatrix.getRowDimension();
        this.N = rawDataMatrix.getColumnDimension();
        this.centeredDataMatrix = new Array2DRowRealMatrix(M,N);
        this.columnMeans = new ArrayRealVector(N);
        double mean;
        RealVector currVec;


        for (int i = 0; i < N; i++){
            currVec = this.dataMatrix.getColumnVector(i);
            mean = 0;
            for (double entry: currVec.toArray()){
                mean += entry;
            }
            mean /= M;
            columnMeans.setEntry(i, mean);
            currVec.mapSubtractToSelf(mean);
            centeredDataMatrix.setColumnVector(i, currVec);
        }

        svd = new SVD(M, N);
        try {
            DenseMatrix cdm = new DenseMatrix(centeredDataMatrix.getData());
            svd = svd.factor(cdm);
            DenseMatrix tm = svd.getVt();
            tm.transpose();
            transformationMatrix = new Array2DRowRealMatrix(Matrices.getArray(tm));
            P = transformationMatrix.getRowDimension();//numRows();
        } catch (NotConvergedException ie) {
            ie.printStackTrace();
        }
    }

    public int getN(){
        return this.N;
    }

    public int getM(){ return this.M; }

    public RealMatrix getTransformationMatrix(){ return this.transformationMatrix; }

    public RealMatrix transform(RealMatrix inputData, int K){
        if (K > Math.min(this.N,this.M)){
            log.warn("Watch your K...K {} M {} Nproc {}", K, this.M, this.N);
        }
        K = Math.min(Math.min(K, this.N), this.M);
        RealMatrix centeredInput = new Array2DRowRealMatrix(inputData.getData());
        RealMatrix transformation = this.transformationMatrix.getSubMatrix(0,this.P-1,0,K-1);
        DenseMatrix ci;
        DenseMatrix transformedData = new DenseMatrix(inputData.getRowDimension(),K);
        DenseMatrix t = new DenseMatrix(transformation.getData());
        RealVector currVec;
        for (int i = 0; i < this.N; i++){
            currVec = inputData.getColumnVector(i);
            currVec.mapSubtractToSelf(this.columnMeans.getEntry(i));
            centeredInput.setColumn(i, currVec.toArray());
        }
        ci = new DenseMatrix(centeredInput.getData());
        ci.mult(t, transformedData);
        return new Array2DRowRealMatrix(Matrices.getArray(transformedData));
    }

}
