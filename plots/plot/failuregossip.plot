set title "Failure Model for Gossip Protocol"
C = "#99ffff"; Cpp = "#4671d5"; Java = "#ff0000"; Python = "#f36e00"
set ylabel 'Percentage Node Convergence'
set xlabel 'Topologies'
set auto x
set yrange [0:0.5]
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set boxwidth 0.9
set xtic scale 0
# 2, 3, 4, 5 are the indexes of the columns; 'fc' stands for 'fillcolor'



plot 'gossip_failure.txt' using 2:xtic(1) ti col fc rgb C, '' u 3 ti col fc rgb Cpp, '' u 4 ti col fc rgb Java, '' u 5 ti col fc rgb Python



