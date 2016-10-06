package macrobase.analysis.stats.optimizer.util;

import macrobase.datamodel.Datum;
import no.uib.cipr.matrix.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PowerMethod {
    private static final Logger log = LoggerFactory.getLogger(PowerMethod.class);

    //private RealMatrix currRawDataMatrix;
    //private RealMatrix currCenteredDataMatrix;
    private RealMatrix currTransformationMatrix; // V, NxK
    private RealVector currColumnMeans;
    //private int currN; //should never change. Always same as raw data
    //private int currM; //this is Nt


    //List<Integer> projectedEigs;
    //List<Integer> unprojectedEigs;
    ///List<RealVector> eigenVects; //last C in list will be the new ones

    public PowerMethod() {
        currTransformationMatrix = new Array2DRowRealMatrix();
    }

    public PowerMethod(RealMatrix data) {
        /*
        this.rawDataMatrix = data;

        this.M = rawDataMatrix.getRowDimension();
        this.N = rawDataMatrix.getColumnDimension();
        this.centeredDataMatrix = new Array2DRowRealMatrix(M,N);
        this.columnMeans = new ArrayRealVector(N);
        double mean;
        RealVector currVec;


        for (int i = 0; i < N; i++){
            currVec = rawDataMatrix.getColumnVector(i);
            mean = 0;
            for (double entry: currVec.toArray()){
                mean += entry;
            }
            mean /= M;
            columnMeans.setEntry(i, mean);
            currVec.mapSubtractToSelf(mean);
            centeredDataMatrix.setColumnVector(i, currVec);
        } */
    }

    /*
    Given a list of datum, we return the raw data matrix
    TODO: Move to utility class
     */
    public RealMatrix extractData(List<Datum> records) {
        ArrayList<double[]> metrics = new ArrayList<>();
        for (Datum d : records) {
            metrics.add(d.metrics().toArray());
        }
        int M = metrics.size();
        int N = metrics.get(0).length;

        double[][] metricArray = new double[M][];
        for (int i = 0; i < M; i++) {
            metricArray[i] = metrics.get(i);
        }
        return new Array2DRowRealMatrix(metricArray);
    }

    /*
    Shuffles the data provided
    TODO: Move to utility class
     */
    public RealMatrix shuffleData(RealMatrix data) {
        int N = data.getColumnDimension();
        int M = data.getRowDimension();
        List<Integer> indicesM = new ArrayList<>();
        int[] indicesN = new int[N];
        for (int i = 0; i < N; i++) {
            indicesN[i] = i;
        }
        for (int i = 0; i < M; i++) {
            indicesM.add(i);
        }
        Collections.shuffle(indicesM);
        int[] iA = ArrayUtils.toPrimitive(indicesM.toArray(new Integer[M]));

        return data.getSubMatrix(iA, indicesN);
    }

    /*
    Compute the LBRCI for the current transformation stored in /this/ on all data
    TODO: Move to utility class
     */
    public double[] getLBRCI(RealMatrix rawData, int numPairs, double qVal, int iter) {
        if (iter == 0) return new double[]{0, 0, 0};
        RealMatrix transformedData = this.transform(rawData);
        int K = transformedData.getColumnDimension(); //will be known by list of eigenthings
        int N = rawData.getColumnDimension();
        int M = rawData.getRowDimension();

        int[] allIndices = new int[N];
        int[] indicesA = new int[numPairs];
        int[] indicesB = new int[numPairs];
        int[] kIndices;

        Random rand = new Random();

        RealVector transformedDists;
        RealVector trueDists;

        List<Double> LBRs;
        double mean = 0;
        double std = 0;
        double slop;

        for (int i = 0; i < numPairs; i++) {
            indicesA[i] = rand.nextInt(M);// - currNt) + currNt;
            indicesB[i] = rand.nextInt(M);//- currNt) + currNt;
            while (indicesA[i] == indicesB[i]) {
                indicesA[i] = rand.nextInt(M);// - currNt) + currNt;
            }
        }

        for (int i = 0; i < N; i++) {
            allIndices[i] = i;
        }
        kIndices = Arrays.copyOf(allIndices, K);

        transformedDists = this.calcDistances(transformedData.getSubMatrix(indicesA, kIndices), transformedData.getSubMatrix(indicesB, kIndices));
        trueDists = this.calcDistances(rawData.getSubMatrix(indicesA, allIndices), rawData.getSubMatrix(indicesB, allIndices));
        LBRs = this.calcLBRList(trueDists, transformedDists);
        for (double l : LBRs) {
            mean += l;
        }
        mean /= numPairs;

        for (double l : LBRs) {
            std += (l - mean) * (l - mean);
        }
        std = Math.sqrt(std / numPairs);
        slop = (qVal * std) / Math.sqrt(numPairs);
        log.debug("LBR {} {} {}, VAR {}", mean-slop, mean, mean+slop, std*std);
        return new double[]{mean - slop, mean, mean + slop, std * std};
    }


    /*
    TODO: move to utility class
     */
    public List<Double> calcLBRList(RealVector trueDists, RealVector transformedDists) {
        int num_entries = trueDists.getDimension();
        List<Double> lbr = new ArrayList<>();
        for (int i = 0; i < num_entries; i++) {
            if (transformedDists.getEntry(i) == 0) {
                if (trueDists.getEntry(i) == 0) lbr.add(1.0); //they were same to begin w/, so max of 1
                else lbr.add(0.0); //can never be negative, so lowest
            } else lbr.add(transformedDists.getEntry(i) / trueDists.getEntry(i));
        }
        return lbr;
    }

    /*
    TODO: move to utility class
     */
    public RealVector calcDistances(RealMatrix dataA, RealMatrix dataB) {
        int rows = dataA.getRowDimension();
        RealMatrix differences = dataA.subtract(dataB);
        RealVector distances = new ArrayRealVector(rows);
        RealVector currVec;
        for (int i = 0; i < rows; i++) {
            currVec = differences.getRowVector(i);
            distances.setEntry(i, currVec.getNorm());
        }
        return distances;
    }

    /*
    TODO: move to utility class
     */
    public int getNextNt(int iter, int currNt) {
        if (iter == 0) {
            return 50;
        } else {
            return currNt * 2;
        }
    }

    public RealMatrix transform(RealMatrix rawData) {
        int K = this.currTransformationMatrix.getColumnDimension();
        RealMatrix centeredInput = new Array2DRowRealMatrix(rawData.getData());
        DenseMatrix ci;
        DenseMatrix transformedData = new DenseMatrix(rawData.getRowDimension(), K);
        DenseMatrix t = new DenseMatrix(currTransformationMatrix.getData());
        RealVector currVec;
        for (int i = 0; i < rawData.getColumnDimension(); i++) {
            currVec = rawData.getColumnVector(i);
            //currVec.mapSubtractToSelf(this.currColumnMeans.getEntry(i));
            centeredInput.setColumn(i, currVec.toArray());
        }
        ci = new DenseMatrix(centeredInput.getData());
        ci.mult(t, transformedData);
        return new Array2DRowRealMatrix(Matrices.getArray(transformedData));
    }

    /*
    Gets your base training matrix for this iteration of Nt
     */
    public RealMatrix computeA(RealMatrix allData, int currNt) {
        return allData.getSubMatrix(0, currNt - 1, 0, allData.getColumnDimension() - 1);
    }

    public RealMatrix initB(RealMatrix A) {
        RealVector currVec;
        RealMatrix B = A.copy();
        for (int i = 0; i < currTransformationMatrix.getColumnDimension(); i++) {
            currVec = currTransformationMatrix.getColumnVector(i);
            B = B.subtract(B.multiply(currVec.outerProduct(currVec)));
        }
        return B;
    }

    /*
    Computes C more eigenvectors from B and appends to currTransformation
     */
    public void calcEigs(RealMatrix B, int c) {
        int N = B.getColumnDimension();
        int K = c;

        int s = currTransformationMatrix.getColumnDimension();

        RealMatrix newTransformationMatrix = new Array2DRowRealMatrix(N, c + currTransformationMatrix.getColumnDimension());
        if (currTransformationMatrix.getColumnDimension() + currTransformationMatrix.getRowDimension() != 0) {
            newTransformationMatrix.setSubMatrix(currTransformationMatrix.getData(), 0, 0);
        }

        DenseMatrix A = new DenseMatrix(N, N);
        DenseMatrix tempData;

        DenseMatrix V = (DenseMatrix) Matrices.random(N, K);//new DenseMatrix(N,N);
        DenseMatrix W = new DenseMatrix(N, K);
        DenseMatrix currQ;
        DenseMatrix prevQ;
        UpperTriangDenseMatrix currR;
        UpperTriangDenseMatrix prevR;

        int iters = 0;

        QR qr = new QR(N, K);

        tempData = new DenseMatrix(B.getData());
        tempData.transAmult(tempData, A); //compute B'B

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

            iters++;

        } while (checkConverge(prevQ, currQ, N, K) > 0.001 && iters < 1000);
        log.debug("Iterations: {}", iters);

        RealMatrix newVects = new Array2DRowRealMatrix(Matrices.getArray(currQ));
        for (int i = 0; i < c; i++){
            newTransformationMatrix.setColumn(s+i, newVects.getColumn(i));
        }
        currTransformationMatrix = newTransformationMatrix;
    }
    public boolean betterThanRandom(RealMatrix B, int c) {
        int s = currTransformationMatrix.getColumnDimension() - c;
        int d = B.getColumnDimension();
        if (s+c > d) {
            log.debug("SOMETHING VERY VERY BAAAD");
        }

        double rand = (double) c / (d - s);

        RealMatrix newProjection = B.multiply(currTransformationMatrix.getSubMatrix(0,d-1,s,s+c-1)); //last c vectors
        double numer = newProjection.transpose().multiply(newProjection).getTrace();
        double denom = B.transpose().multiply(B).getTrace();

        log.debug("random {} we have {}", rand, numer/denom);
        return numer/denom >= rand;
    }

    /*
    project off the components from the last iteration
     */
    public RealMatrix updateB(RealMatrix B, int c) {
        int d = currTransformationMatrix.getRowDimension(); //N
        int s = currTransformationMatrix.getColumnDimension() - c; //K (c*numiters)

        RealVector currVec;
        RealMatrix Bnew = B.copy();
        for (int i = 0; i < c; i++) {
            currVec = currTransformationMatrix.getColumnVector(s+i);
            Bnew = Bnew.subtract(B.multiply(currVec.outerProduct(currVec)));
        }
        return Bnew;
    }

////////////////////////////////

    /*
    Computes the column means, and column-mean-subtracted data

    public void centerData() {
        this.centeredDataMatrix = new Array2DRowRealMatrix(M,N);
        this.columnMeans = new ArrayRealVector(N);
        double mean;
        RealVector currVec;

        for (int i = 0; i < N; i++){
            currVec = rawDataMatrix.getColumnVector(i);
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
    */



    public List<RealVector> refine(RealMatrix dataMatrix, List<RealVector> eigenVects) {
        return new ArrayList<>();
    }


    /*
    Pass in the matrix for which you want to compute right singular vectors for
     */
    public RealMatrix computeEigs(RealMatrix dataMatrix, int Nt, int N, int K) {
        //N = 3;
        //K = 2;
        DenseMatrix A = new DenseMatrix(N, N);
        DenseMatrix tempData;

        DenseMatrix V = (DenseMatrix) Matrices.random(N, K);//new DenseMatrix(N,N);
        DenseMatrix W = new DenseMatrix(N, K);
        DenseMatrix currQ;
        DenseMatrix prevQ;
        UpperTriangDenseMatrix currR;
        UpperTriangDenseMatrix prevR;

        DenseMatrix checkConverge = new DenseMatrix(N, N);
        DenseMatrix eye = Matrices.identity(N);

        int iters = 0;

        QR qr = new QR(N, K);

        Random rand = new Random();

        tempData = new DenseMatrix(dataMatrix.getSubMatrix(0, Nt - 1, 0, N - 1).getData());
        tempData.transAmult(tempData, A);

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

            iters++;

        } while (checkConverge(prevQ, currQ, N, K) > 0.001 && iters < 1000);
        log.debug("Iterations: {}", iters);
        return new Array2DRowRealMatrix(currQ.getData());

    }

    private double checkConverge(DenseMatrix prevQ, DenseMatrix currQ, int N, int K) {
        DenseMatrix check = new DenseMatrix(K, K);
        DenseMatrix eye = Matrices.identity(K);

        currQ.transAmult(prevQ, check);
        for (int i = 0; i < K; i++) {
            check.set(i, i, Math.abs(check.get(i, i)));
        }
        check.add(-1, eye);

        return check.norm(Matrix.Norm.Infinity);
    }

}
