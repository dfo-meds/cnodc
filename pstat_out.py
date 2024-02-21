import pstats
import sys

p = pstats.Stats(sys.argv[1])
p.strip_dirs()
p.sort_stats(pstats.SortKey.TIME)
p.print_stats(20)
p.sort_stats(pstats.SortKey.CUMULATIVE)
p.print_stats(5)
