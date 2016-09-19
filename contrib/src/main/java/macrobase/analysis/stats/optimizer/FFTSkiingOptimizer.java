package macrobase.analysis.stats.optimizer;

import org.apache.commons.math3.linear.RealMatrix;

public class FFTSkiingOptimizer extends SkiingOptimizer {

    public FFTSkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
    }

    @Override
    public void fit(int Nt) {

    }

    @Override
    public RealMatrix transform(int K) {
        return null;
    }

    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        return null;
    }
}
