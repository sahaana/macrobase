package macrobase.analysis.stats.optimizer;

import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public abstract class Optimizer {
    protected int N; //orig dimension
    protected int M; //orig number of training samples
    protected ArrayList<Integer> NtList;
    protected double epsilon;
    protected RealMatrix dataMatrix;

    public Optimizer(double epsilon){
        this.epsilon = epsilon;
        this.NtList = new ArrayList<>();
    }

    public int getN(){return N;}

    public int getM(){return M;}

    public int getNtList(int i){return NtList.get(i);}

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
        this.dataMatrix = new Array2DRowRealMatrix(metricArray);
    }

    public RealVector calcDistances(RealMatrix dataA, RealMatrix dataB){
        int rows = dataA.getRowDimension();
        RealMatrix differences = dataA.subtract(dataB);
        RealVector distances = new ArrayRealVector(rows);
        RealVector currVec;
        for (int i = 0; i < rows; i++){
            currVec = differences.getRowVector(i);
            distances.setEntry(i, currVec.getNorm());
        }
        return distances;
    }

    public double LBR(RealVector trueDists, RealVector transformedDists){
        int num_entries = trueDists.getDimension();
        double lbr = 0;
        for (int i = 0; i < num_entries; i++) {
            if (transformedDists.getEntry(i) == 0){
                if (trueDists.getEntry(i) == 0) lbr += 1; //they were same to begin w/, so max of 1
                else lbr += 0; //can never be negative, so lowest
            }
            else lbr += trueDists.getEntry(i)/transformedDists.getEntry(i);
        }

        //arbitrarily choose to average all of the LBRs
        return lbr/num_entries;
    }

    public abstract RealMatrix transform(int K, int Nt);

    public abstract double epsilonAttained(int iter, RealMatrix transformedMatrix);

    public abstract int getNextNt(int iter, int K);
}
