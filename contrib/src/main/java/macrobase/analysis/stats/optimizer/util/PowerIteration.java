package macrobase.analysis.stats.optimizer.util;

import macrobase.analysis.stats.optimizer.PAASkiingOptimizer;
import no.uib.cipr.matrix.*;
import org.apache.commons.math3.analysis.function.Power;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PowerIteration {
    private static final Logger log = LoggerFactory.getLogger(PowerIteration.class);
    private RealMatrix dataMatrix;

    public PowerIteration(RealMatrix dataMatrix){
        this.dataMatrix = dataMatrix;
    }

    public RealMatrix computeEigs(int Nt, int N, int K) {
        //N = 3;
        //K = 2;
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

        tempData = new DenseMatrix(dataMatrix.getSubMatrix(0, Nt-1, 0, N-1).getData());
        tempData.transAmult(tempData,A);

        //A = new DenseMatrix(new double[][]{{1, 3, 4}, {3, 1, 5}, {4, 5, 5}});
        //N = 3;

        /*for (int i = 0; i < N; i++){
            for (int j = 0; j < N; j++) {
                V.set(i,j,rand.nextGaussian());
            }
        } */
        //V = Matrices.identity(N);

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

        } while (checkConverge(prevQ, currQ, N, K) > 0.001 && iters < 1000);
        log.debug("Iterations: {}", iters);
        return new Array2DRowRealMatrix(currQ.getData());

    }

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
