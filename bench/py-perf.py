import pstats
p = pstats.Stats('out.pstats')
p.strip_dirs()
#p.sort_stats('cumtime')
p.sort_stats('cumtime') # 基于对time排序
p.print_stats(5000)
