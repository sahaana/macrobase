package macrobase.analysis.stats.optimizer.util;

import no.uib.cipr.matrix.*;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PCAFast implements PCA{
    private static final Logger log = LoggerFactory.getLogger(PCAFast.class);

    private RealMatrix dataMatrix;
    private RealMatrix centeredDataMatrix;
    private RealVector columnMeans;
    private RealMatrix transformation;
    private int M;
    private int N;
    private boolean init; //flag to see if this has not been used to transform already

    public PCAFast(RealMatrix rawDataMatrix){
        this.dataMatrix = rawDataMatrix;
        this.M = rawDataMatrix.getRowDimension();
        this.N = rawDataMatrix.getColumnDimension();
        this.centeredDataMatrix = new Array2DRowRealMatrix(M,N);
        this.columnMeans = new ArrayRealVector(N);
        this.init = true;

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
    }

    public int getN(){
        return this.N;
    }

    public int getM(){ return this.M; }

    public RealMatrix getTransformationMatrix(){ return this.transformation; }

    public RealMatrix transform(RealMatrix inputData, int K) {
        if (K > Math.min(this.N, this.M)) {
            log.warn("Watch your K...K {} M {} Nproc {}", K, this.M, this.N);
        }
        K = Math.min(Math.min(K, this.N), this.M);

        transformation = new Array2DRowRealMatrix(this.N, K);
        RealMatrix centeredInput = new Array2DRowRealMatrix(inputData.getData());

        RealVector currVec;

        DenseMatrix ci; //centeredInput
        DenseMatrix omega = new DenseMatrix(N, K); //random initializer matrix
        DenseMatrix Y1 = new DenseMatrix(M, K); //intermediate matrix
        DenseMatrix Y2 = new DenseMatrix(N, K);
        DenseMatrix transformedData = new DenseMatrix(M, K);
        DenseMatrix tm;

        Random rand = new Random();

        QR qr = new QR(N, K);

        //generate gaussian random initialization matrix
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < K; j++)
                omega.set(i, j, rand.nextGaussian());
        }

        //center input data
        for (int i = 0; i < this.N; i++) {
            currVec = inputData.getColumnVector(i);
            currVec.mapSubtractToSelf(this.columnMeans.getEntry(i));
            centeredInput.setColumn(i, currVec.toArray());
        }
        ci = new DenseMatrix(centeredInput.getData());

        //run one step power iteration
        ci.mult(omega, Y1);
        ci.transAmult(Y1, Y2);
        qr.factor(Y2);
        tm = qr.getQ();

        transformation = new Array2DRowRealMatrix(Matrices.getArray(tm));

        //transform input data and return
        ci.mult(tm, transformedData);

        return new Array2DRowRealMatrix(Matrices.getArray(transformedData));
    }

}
