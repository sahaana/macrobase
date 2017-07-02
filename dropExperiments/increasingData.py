#!/usr/bin/env python
import subprocess

d0 = 'synth'
d1 = 'synth'
d2 = 'synth_15000'
d3 = 'synth_45000'
d4 = 'synth_135000'

Ds = [25, 50, 100, 200, 300]
Ms = [5000, 15000, 45000, 135000]
LBR = 0.98
q = 1.96
Kexp = 2
work = "REUSE"
opt = "OPTIMIZE"

proc = "java -Xms6g ${JAVA_OPTS} -cp 'assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes' macrobase.analysis.stats.optimizer.experiments.IncreasingDatasizeExperiments %s %s %s %s %s %f %f %d %s %s"
comp = "mvn compile -Dmaven.test.skip=true"
package = "mvn package -DskipTests"
subprocess.call(comp, shell=True)
subprocess.call(package, shell=True)

#for d in Ds:
#d0 = "troll_{}".format(d)
#d1,d2,d3,d4 = ["troll_{}_{}".format(M,d) for M in Ms]
subprocess.call(proc % (d0,d1,d2,d3,d4, LBR, q, Kexp, work, opt), shell=True)
