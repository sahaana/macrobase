package macrobase.analysis.stats.optimizer.util;

import no.uib.cipr.matrix.*;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PCAPowerIteration {
    private static final Logger log = LoggerFactory.getLogger(PCAPowerIteration.class);

    private RealMatrix dataMatrix;
    private RealMatrix centeredDataMatrix;
    private RealVector columnMeans;
    private RealMatrix largestTransform;
    private int M;
    private int N;
    private boolean init; //flag to see if this has not been used to transform already

    public PCAPowerIteration(RealMatrix rawDataMatrix){
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

    public RealMatrix getLargestTransform(){ return this.largestTransform; }

    public RealMatrix transform(RealMatrix inputData, int K){
        if (K > Math.min(this.N,this.M)){
            log.warn("Watch your K...K {} M {} Nproc {}", K, this.M, this.N);
        }
        K = Math.min(Math.min(K, this.N), this.M);

        RealMatrix transformation = new Array2DRowRealMatrix(N, K);
        RealVector currVec;
        DenseMatrix t;
        DenseMatrix ci;
        DenseMatrix transformedData;

        RealMatrix centeredInput = new Array2DRowRealMatrix(inputData.getData());
        //if you've never transformed anything with PI before, compute from scratch
        if (init){
            init = false;
            this.largestTransform = this.computeEigs(K);
        }

        // if the largest transformation you've done is not big enough, pad with random and compute the difference
        int randomPadding = K - this.largestTransform.getColumnDimension();
        if (randomPadding > 0){
            transformation.setSubMatrix(this.largestTransform.getData(),0,0);
            for(int i = 0; i < randomPadding; i++){
                for(int j = 0; j < N; j++){
                    transformation.setEntry(i,j,Math.random());
                }
            }
            this.largestTransform = this.computeEigs(K, transformation);
        }

        transformation = this.largestTransform.getSubMatrix(0,this.N-1,0,K-1); // Remember this came from PCA and was P-1
        t = new DenseMatrix(transformation.getData());
        transformedData = new DenseMatrix(inputData.getRowDimension(),K);

        for (int i = 0; i < this.N; i++){
            currVec = inputData.getColumnVector(i);
            currVec.mapSubtractToSelf(this.columnMeans.getEntry(i));
            centeredInput.setColumn(i, currVec.toArray());
        }
        ci = new DenseMatrix(centeredInput.getData());
        ci.mult(t, transformedData);
        return new Array2DRowRealMatrix(Matrices.getArray(transformedData));
    }

    public RealMatrix computeEigs(int K, RealMatrix initV) {
        //DenseMatrix A = new DenseMatrix(N,N);
        DenseMatrix temp = new DenseMatrix(M, K);
        DenseMatrix centered;
        DenseMatrix centeredT = new DenseMatrix(N, M);

        DenseMatrix V = new DenseMatrix(initV.getData()); //N x K matrix
        DenseMatrix W = new DenseMatrix(N,K);
        DenseMatrix currQ;
        DenseMatrix prevQ;
        UpperTriangDenseMatrix currR;
        UpperTriangDenseMatrix prevR;

        int iters = 0;

        QR qr = new QR(N, K);

        centered = new DenseMatrix(centeredDataMatrix.getData());
        centered.transpose(centeredT);
        //centered.transAmult(centered,A); // A = mlX'*mlX

        qr.factor(V);
        currQ = qr.getQ();
        currR = qr.getR();

        do {
            prevQ = currQ.copy();
            prevR = currR.copy();

            centered.mult(prevQ,temp); // mlX*prevQ = temp
            centeredT.mult(temp, W); // mlX'*temp = mlX'*mlX*prevQ = W

            //A.mult(prevQ, W); // mlX'*mlX*prevQ = W
            qr.factor(W);
            currQ = qr.getQ();
            currR = qr.getR();
        } while (iters++ < 3000 && checkConverge(prevQ, currQ, N, K) > 0.001);
        log.debug("Iterations: {}", iters);
        return new Array2DRowRealMatrix(Matrices.getArray(currQ));

    }


    public RealMatrix computeEigs(int K) {
        double[][] randV = new double[N][K];
        for (int i = 0; i < N; i++){
            for (int j = 0; j < K; j++){
                randV[i][j] = Math.random();
            }
        }
        RealMatrix V = new Array2DRowRealMatrix(randV);
        return computeEigs(K, V);

    }

    //TODO: not stable as order not guaranteed in theory
    private double checkConverge(DenseMatrix prevQ, DenseMatrix currQ, int N, int K) {
        DenseMatrix check = new DenseMatrix(K,K);
        DenseMatrix eye = Matrices.identity(K);

        currQ.transAmult(prevQ, check);
        for (int i = 0; i < K; i++){
            check.set(i, i, Math.abs(check.get(i, i)));
        }
        check.add(-1, eye);

        return check.norm(Matrix.Norm.Infinity);
    }

}
