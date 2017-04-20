package macrobase.analysis.stats.optimizer.util;

import no.uib.cipr.matrix.*;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PowerIteration {
    private static final Logger log = LoggerFactory.getLogger(PowerIteration.class);
    private RealMatrix dataMatrix;
    private RealMatrix centeredDataMatrix;
    private RealVector columnMeans;
    private int M;
    private int N;

    public PowerIteration(RealMatrix rawDataMatrix){
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
    }

    public RealMatrix computeEigs(int Nt, int N, int K) {
        DenseMatrix A = new DenseMatrix(N,N);
        DenseMatrix tempData;

        DenseMatrix V = (DenseMatrix) Matrices.random(N,K);//new DenseMatrix(N,N);
        DenseMatrix W = new DenseMatrix(N,K);
        DenseMatrix currQ;
        DenseMatrix prevQ;
        UpperTriangDenseMatrix currR;
        UpperTriangDenseMatrix prevR;

        DenseMatrix checkConverge = new DenseMatrix(N,N);
        DenseMatrix eye = Matrices.identity(N);

        int iters = 0;

        QR qr = new QR(N, K);

        Random rand = new Random();

        tempData = new DenseMatrix(centeredDataMatrix.getSubMatrix(0, Nt-1, 0, N-1).getData());
        tempData.transAmult(tempData,A); //TODO: don't compute this at once

        qr.factor(V);
        currQ = qr.getQ();
        currR = qr.getR();

        do {
            prevQ = currQ.copy();
            prevR = currR.copy();

            A.mult(prevQ, W);
            qr.factor(W);
            currQ = qr.getQ();
            currR = qr.getR();

            //currQ.transAmult(prevQ, checkConverge);
            //checkConverge.add(-1, eye);
            //log.debug("Checking Convergence {}",checkConverge(prevQ, currQ, N, K));
            iters++;

        } while (checkConverge(prevQ, currQ, N, K) > 0.001 && iters < 3000);
        log.debug("Iterations: {}", iters);
        return new Array2DRowRealMatrix(Matrices.getArray(currQ));

    }

    //TODO: not stable. order not guaranteed
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
