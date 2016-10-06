package macrobase.analysis.stats.optimizer.util;

//import com.sun.tools.javac.util.Assert;
import no.uib.cipr.matrix.*;
import org.apache.commons.math3.linear.*;

//import org.jblas.DoubleMatrix;
//import org.jblas.Singular;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import static org.junit.Assert.assertTrue;


public class PCA {
    private static final Logger log = LoggerFactory.getLogger(PCA.class);

    private RealMatrix dataMatrix; // A
    private RealMatrix centeredDataMatrix; // X
    private RealMatrix transformationMatrix; // V
    private RealVector columnMeans;
    private SVD svd; //gives X = UDV', U=mxp D=pxp V = pxn
    //private RealMatrix cachedTransform;
    private int N;
    private int M;
    private int P;

    public PCA(RealMatrix rawDataMatrix) {
        this.dataMatrix = rawDataMatrix;
        this.M = rawDataMatrix.getRowDimension();//.numRows();
        this.N = rawDataMatrix.getColumnDimension();//numColumns();
        this.centeredDataMatrix = new Array2DRowRealMatrix(M,N);//rawDataMatrix.copy();//new DenseMatrix(M,N);
        this.columnMeans = new ArrayRealVector(N);//DenseVector(N);
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

    public RealMatrix getTransformationMatrix(){ return this.transformationMatrix; }

    public RealMatrix transform(RealMatrix inputData, int K){
        /*if (K > Math.min(this.N,this.M)){
          log.warn("Watch your K...K {} M {} Nproc {}", K, this.M, this.N);
        }
        K = Math.min(Math.min(K, this.N), this.M);
        RealMatrix centeredInput = new Array2DRowRealMatrix(inputData.getData());
        RealMatrix transformation = this.transformationMatrix.getSubMatrix(0,this.P-1,0,K-1);
        RealMatrix transformedData = new DenseMatrix(inputData.numRows(),K);

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
        */
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
