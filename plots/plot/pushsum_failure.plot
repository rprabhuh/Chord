set title "Failure Model for Pushsum Protocol"
Five = "#99ffff"; Ten = "#4671d5"; Fifteen = "#ff0000"; Twenty = "#f36e00"
set ylabel 'Convergence Time (ms)'
set xlabel 'Topologies'
set auto x
set yrange [0:2500]
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set boxwidth 0.9
set xtic scale 0
# 2, 3, 4, 5 are the indexes of the columns; 'fc' stands for 'fillcolor'



plot 'pushsum_failure.txt' using 2:xtic(1) ti col fc rgb Five, '' u 3 ti col fc rgb Ten, '' u 4 ti col fc rgb Fifteen, '' u 5 ti col fc rgb Twenty



