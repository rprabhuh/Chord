set yrange [0:100]
set style line 2 lc rgb "blue"
set boxwidth 0.5
set style fill solid
set ylabel 'Percentage Of Queries Successful'
set xlabel 'Percentage Node Failure'
set title "Failure Models - Permanent Failure"
plot "permanentFailure.dat" using 1:3:xtic(2) with boxes ls 2
