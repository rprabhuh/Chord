set title 'Gossip Protocol Simulation'

set xlabel 'Number of Nodes'

set ylabel 'Time taken for convergence (s)'

set logscale x
set logscale y
set logscale x 10
set logscale y 10

set terminal jpeg

set output 'Gossip.jpg'

plot "gossip.txt" using 1:2 title 'Full' with linespoints, "gossip.txt" using 1:3 title '3D' with linespoints, "gossip.txt" using 1:4 title 'Line' with linespoints, "gossip.txt" using 1:5 title 'Imperfect 3D' with linespoints


