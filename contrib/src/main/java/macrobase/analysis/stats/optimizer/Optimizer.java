package macrobase.analysis.stats.optimizer;

import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

import Jama.Matrix;
import org.apache.commons.lang3.ObjectUtils;
import org.eclipse.jetty.util.log.Log;
import org.jblas.DoubleMatrix;
import weka.core.matrix.DoubleVector;

public abstract class Optimizer {
    protected int N; //orig dimension
    protected int M; //orig number of training samples
    protected ArrayList<Integer> NtList;
    protected double epsilon;
    protected Matrix dataMatrix;

    public Optimizer(double epsilon){
        this.epsilon = epsilon;
        this.NtList = new ArrayList<>();
    }

    public int getN(){return N;}

    public int getM(){return M;}

    public void extractData(List<Datum> records){
        ArrayList<double[]> metrics = new ArrayList<>();
        for (Datum d: records) {
            metrics.add(d.metrics().toArray());
        }
        this.M = metrics.size();
        this.N = metrics.get(0).length;

        double[][] metricArray = new double[M][];
        for (int i = 0; i < M; i++){
            metricArray[i] = metrics.get(i);
        }
        this.dataMatrix = new Matrix(metricArray);
    }

    public DoubleVector calcDistances(Matrix dataA, Matrix dataB){
        DoubleMatrix differences;
        DoubleMatrix squaredSum;
        DoubleVector norms;

        differences = new DoubleMatrix((dataA.minus(dataB)).getArray());
        squaredSum = differences.muli(differences);
        norms = (new DoubleVector(squaredSum.rowSums().toArray())).sqrt();

        return norms;
    }

    public double LBR(DoubleVector trueDists, DoubleVector tranformedDists){
        return trueDists.dividedBy(tranformedDists).sum()/tranformedDists.size();
    }

    public abstract Matrix transform(int K, int Nt);

    public abstract double epsilonAttained(int iter, Matrix transformedMatrix);

    public abstract int getNextNt(int iter);
}
