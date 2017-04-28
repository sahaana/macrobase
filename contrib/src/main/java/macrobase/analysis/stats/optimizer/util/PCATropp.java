package macrobase.analysis.stats.optimizer.util;

import no.uib.cipr.matrix.*;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PCATropp implements PCA{
    private static final Logger log = LoggerFactory.getLogger(PCATropp.class);

    private RealMatrix dataMatrix;
    private RealMatrix centeredDataMatrix;
    private RealVector columnMeans;
    private RealMatrix transformation;
    private int M;
    private int N;
    private boolean init; //flag to see if this has not been used to transform already
    private int p; //small additional number of columns to pull. Either 5, 10 or L
    private int q; //small number of power iterations. Typically 1-3
    private int highestK; //largest K obtained with this data

    public PCATropp(RealMatrix rawDataMatrix){
        this.p = 5;
        this.q = 2;
        this.highestK = 0;
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
        int Kp = Math.min(Math.min(K + p, this.N), this.M);

        RealMatrix centeredOutput = new Array2DRowRealMatrix(inputData.getData());
        RealVector currVec;
        DenseMatrix co; //centeredOutput
        DenseMatrix tm; //transformation matrix
        DenseMatrix transformedData = new DenseMatrix(inputData.getRowDimension(), K);

        //if the K you want is higher than what you've seen compute transform from scratch
        if (K > highestK){
            highestK = K;
            transformation = new Array2DRowRealMatrix(this.N, K);

            DenseMatrix ci; //centeredInput
            DenseMatrix omega = new DenseMatrix(N, Kp); //random initializer matrix
            DenseMatrix Y1 = new DenseMatrix(M, Kp); //intermediate matrix
            DenseMatrix Y2 = new DenseMatrix(N, Kp); //intermediate matrix
            DenseMatrix B = new DenseMatrix(Kp, N); //intermediate matrix


            Random rand = new Random();

            QR qr = new QR(M, Kp);
            SVD svd = new SVD(Kp, N);

            //generate gaussian random initialization matrix
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < Kp; j++)
                    omega.set(i, j, rand.nextGaussian());
            }

            ci = new DenseMatrix(centeredDataMatrix.getData());

            //run some modded power iteration
            ci.mult(omega, Y1);
            for (int i = 0; i < q; i++) {
                ci.transAmult(Y1, Y2);
                ci.mult(Y2, Y1);

                qr.factor(Y1);
                Y1 = qr.getQ();
            }

            //SVD to get the top K
            Y1.transAmult(ci, B);
            //svd = svd.factor(B);
            try {
                svd = svd.factor(B);
                tm = svd.getVt();
                tm.transpose();
                transformation = new Array2DRowRealMatrix(Matrices.getArray(tm)).getSubMatrix(0, N-1, 0, K-1);
            } catch (NotConvergedException ie) {
                ie.printStackTrace();
            }
        }

        //center, transform input data and return
        for (int i = 0; i < this.N; i++) {
            currVec = inputData.getColumnVector(i);
            currVec.mapSubtractToSelf(this.columnMeans.getEntry(i));
            centeredOutput.setColumn(i, currVec.toArray());
        }
        co = new DenseMatrix(centeredOutput.getData());

        tm = new DenseMatrix(transformation.getSubMatrix(0, N-1, 0, K-1).getData());
        co.mult(tm, transformedData);

        return new Array2DRowRealMatrix(Matrices.getArray(transformedData));
    }

}
