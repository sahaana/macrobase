package macrobase.analysis.stats.optimizer;

import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

import Jama.Matrix;

public abstract class Optimizer {
    protected int N; //orig dimension
    protected int M; //orig number of training samples
    protected ArrayList<Integer> NtList;
    protected double epsilon;
    protected Matrix dataMatrix;

    public Optimizer(double epsilon){
        this.epsilon = epsilon;
    }

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

    public abstract Matrix tranform(int K, int Nt);

    public abstract double epsilonAttained(Matrix transformedMatrix);

    public abstract int getNextNt(int iter);
}
