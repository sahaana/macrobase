package macrobase.analysis.stats.optimizer.util;

import com.sun.tools.javac.util.Assert;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.NotConvergedException;
import no.uib.cipr.matrix.SVD;
//import org.apache.commons.math3.linear.*;

//import org.jblas.DoubleMatrix;
//import org.jblas.Singular;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;


public class PCA {
    private static final Logger log = LoggerFactory.getLogger(PCA.class);

    private DenseMatrix dataMatrix; // A
    private DenseMatrix centeredDataMatrix; // X
    private DenseMatrix transformationMatrix; // V
    private DenseVector columnMeans;
    private SVD svd; //gives X = UDV', U=mxp D=pxp V = pxn
    //private RealMatrix cachedTransform;
    private int N;
    private int M;
    private int P;

    public PCA(DenseMatrix rawDataMatrix) {
        this.dataMatrix = rawDataMatrix;
        this.M = rawDataMatrix.numRows();
        this.N = rawDataMatrix.numColumns();
        this.centeredDataMatrix = rawDataMatrix.copy();//new DenseMatrix(M,N);
        this.columnMeans = new DenseVector(N);
        double mean;

        for (int j = 0; j < N; j++){
            //calculate column means
            mean = 0;
            for (int i = 0; i < M; i++){
                mean += dataMatrix.get(i,j);
            }
            mean /= N;
            columnMeans.set(j, mean);

            //subtract the column average from original matrix
            for (int i = 0; i < M; i++) {
                centeredDataMatrix.add(i, j, -mean);
            }
        }

        //svd = new SVD(M, N);
        try {
            svd = svd.factor(centeredDataMatrix);
            transformationMatrix = svd.getVt();
            transformationMatrix.transpose();
            P = transformationMatrix.numRows();
        } catch (NotConvergedException ie) {
            ie.printStackTrace();
        }

        /*
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


        //this is faster, but more annoying to integrate
        DenseMatrix sing2 =  new DenseMatrix(M,N);
        DenseMatrix tcent2 = new DenseMatrix(centeredDataMatrix.getData());
        svd = new SVD(this.M, this.N);
        try {
            svd = svd.factor(tcent2);
            sing2 = svd.getVt();
            sing2.transpose();
        } catch (NotConvergedException ie){ ie.printStackTrace(); }


        //SVD = new SingularValueDecomposition(centeredDataMatrix);
        //this.transformationMatrix = SVD.getV();

       // this.transformationMatrix = new Array2DRowRealMatrix(sing.toArray2());
        this.transformationMatrix = new Array2DRowRealMatrix(sing2.numRows(), sing2.numColumns());
        for (int i = 0; i < sing2.numRows(); i++){
            for (int j = 0; j < sing2.numColumns(); j++){
                this.transformationMatrix.setEntry(i,j,sing2.get(i,j));
            }
        }
        this.P = this.transformationMatrix.getRowDimension();
        */
    }

    public int getN(){
        return this.N;
    }

    public int getM(){ return this.M; }

    public DenseMatrix transform(DenseMatrix inputData, int K){
        if (K > Math.min(this.N,this.M)){
          log.warn("Watch your K...K {} M {} Nproc {}", K, this.M, this.N);
        }
        K = Math.min(Math.min(K, this.N), this.M);
        DenseMatrix centeredInput = inputData.copy();//new Array2DRowRealMatrix(inputData.getData());
        DenseMatrix transformation = new DenseMatrix(P,K);//this.transformationMatrix.getSubMatrix(0,this.P-1,0,K-1);
        DenseMatrix transformedData = new DenseMatrix(inputData.numRows(),K);

        //TODO: do a deep copy by checking if it's full transform. It mostly will be...
        for (int i = 0; i < P; i++){
            for (int j = 0; j < K; j++){
                transformation.set(i,j,transformationMatrix.get(i,j));
            }
        }

        //centering input
        for (int j = 0; j < inputData.numColumns(); j++){
            for (int i = 0; i < inputData.numRows(); i++){
                centeredInput.add(i,j,-columnMeans.get(j));
            }
        }

        //computing transformation
        centeredInput.mult(transformation, transformedData);
        return transformedData;

        /*

        for (int i = 0; i < this.N; i++){
            currVec = inputData.getColumnVector(i);
            currVec.mapSubtractToSelf(this.columnMeans.getEntry(i));
            centeredInput.setColumn(i, currVec.toArray());
        }
        DenseMatrix t = new DenseMatrix(centeredInput.getData());
        DenseMatrix t2 = new DenseMatrix(transformation.getData());
        DenseMatrix t3 = new DenseMatrix(inputData.getRowDimension(),K);
        t.mult(t2,t3);

        //DoubleMatrix s = new DoubleMatrix(centeredInput.getData());
        //DoubleMatrix s2 = new DoubleMatrix(transformation.getData());
        //DoubleMatrix s3 = s.mmul(s2);

        RealMatrix u = new Array2DRowRealMatrix(inputData.getRowDimension(),K);//s3.toArray2());//centeredInput.multiply(transformation);
        for (int i = 0; i < inputData.getRowDimension(); i++){
            for (int j = 0; j < K; j++){
                //assertTrue("string",Math.abs(t.get(i,j)-s3.get(i,j)) < .0001);
                //assertTrue("string", Math.abs(u.getEntry(i,j)-t.get(i,j)) < .0001);
                u.setEntry(i,j,t3.get(i,j));
            }
        }


        return u;*/
    }

}
