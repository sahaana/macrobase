#!/usr/bin/env python
import subprocess

dataset1 = 'synth'
dataset2 = 'synth_5000'
dataset3 = 'synth_10000'
dataset4 = 'synth_15000'
LBR = 0.98
q = 1.96
Kexp = 2
work = "REUSE"
opt = "OPTIMIZE"

proc = "java -Xms6g ${JAVA_OPTS} -cp 'assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes' macrobase.analysis.stats.optimizer.experiments.IncreasingDatasizeExperiments %s %s %s %s %f %f %d %s %s"
comp = "mvn compile -Dmaven.test.skip=true"
package = "mvn package -DskipTests"
subprocess.call(comp, shell=True)
subprocess.call(package, shell=True)

for dataset in datasets:
    subprocess.call(proc % (dataset1,dataset2,dataset3,dataset4, LBR, q, Kexp, work, opt), shell=True)
