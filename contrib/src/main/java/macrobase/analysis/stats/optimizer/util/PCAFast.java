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
    private int highestK; //highest thing transformed so far

    public PCAFast(RealMatrix rawDataMatrix){
        this.dataMatrix = rawDataMatrix;
        this.M = rawDataMatrix.getRowDimension();
        this.N = rawDataMatrix.getColumnDimension();
        this.centeredDataMatrix = new Array2DRowRealMatrix(M,N);
        this.columnMeans = new ArrayRealVector(N);
        this.highestK = 0;

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

        RealMatrix centeredOutput = new Array2DRowRealMatrix(inputData.getData());
        RealVector currVec;
        DenseMatrix co; //centered output
        DenseMatrix tm; //transformation matrix
        DenseMatrix transformedData = new DenseMatrix(inputData.getRowDimension(), K);

        //if the K you want is higher than what you've seen compute transform from scratch
        if (K > highestK) {
            highestK = K;
            transformation = new Array2DRowRealMatrix(this.N, K);

            DenseMatrix ci; //centeredInput
            DenseMatrix omega = new DenseMatrix(N, K); //random initializer matrix
            DenseMatrix Y1 = new DenseMatrix(M, K); //intermediate matrix
            DenseMatrix Y2 = new DenseMatrix(N, K);

            Random rand = new Random();

            QR qr = new QR(N, K);

            //generate gaussian random initialization matrix
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < K; j++)
                    omega.set(i, j, rand.nextGaussian());
            }

            //run one step power iteration
            ci = new DenseMatrix(centeredDataMatrix.getData());
            ci.mult(omega, Y1);
            ci.transAmult(Y1, Y2);
            qr.factor(Y2);
            tm = qr.getQ();
            transformation = new Array2DRowRealMatrix(Matrices.getArray(tm)).getSubMatrix(0, N-1, 0, K-1);

        }

        //center, transform input data and return
        for (int i = 0; i < this.N; i++) {
            currVec = inputData.getColumnVector(i);
            currVec.mapSubtractToSelf(this.columnMeans.getEntry(i));
            centeredOutput.setColumn(i, currVec.toArray());
        }
        co = new DenseMatrix(centeredOutput.getData());

        tm =  new DenseMatrix(transformation.getSubMatrix(0, N-1, 0, K-1).getData());
        co.mult(tm, transformedData);

        return new Array2DRowRealMatrix(Matrices.getArray(transformedData));
    }

}
