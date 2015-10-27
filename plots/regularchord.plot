set nokey
set yrange [0:30]
set style line 2 lc rgb "blue"
set boxwidth 0.5
set style fill solid
set ylabel 'Average Number of Hops'
set xlabel 'Number of Nodes'
set title "Regular Chord Implementation"
plot "regularchord.dat" using 1:3:xtic(2) with boxes ls 2
