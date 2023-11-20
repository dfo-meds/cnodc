import pstats
import sys

p = pstats.Stats(sys.argv[1])
p.sort_stats(pstats.SortKey.TIME).print_stats(20)
