# frozen_string_literal: true


NoSE::TimeDependWorkload.new do
  #Model 'tpch'
  Model 'tpch_card'

  def step_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    middle_ts = timesteps / 2
    (0..timesteps).map do |current_ts|
      current_ts <= middle_ts ? start_ratio : end_ratio
    end
  end

  step = step_freq(0.001, 0.999, 6)
  frequencies = step

  TimeSteps frequencies.size
  Interval 7200 # specify interval in minutes
  #Static true
  #FirstTs true
  #LastTs true

  Group 'Group1', default: frequencies.reverse do
    #Q 'INSERT INTO lineitem SET l_linenumber=?, l_quantity=?, l_extendedprice=?, l_discount=?, ' \
    #              'l_tax = ?, l_returnflag=?, l_linestatus=?, l_shipdate=?, l_commitdate=?, l_receiptdate=?, ' \
    #              'l_shipmode=?, l_comment=?, dummy=? AND CONNECT TO l_partkey(?), l_orderkey(?) -- lineitem_insert'

    Q 'SELECT ps_suppkey.s_acctbal, ps_suppkey.s_name, s_nationkey.n_name, part.p_partkey, part.p_mfgr, '\
         'ps_suppkey.s_address, ps_suppkey.s_phone, ps_suppkey.s_comment ' \
       'FROM part.from_partsupp.ps_suppkey.s_nationkey.n_regionkey ' \
       'WHERE part.p_size = ? AND part.p_type = ? AND n_regionkey.r_name = ? AND from_partsupp.ps_supplycost = ? '\
       'ORDER BY ps_suppkey.s_acctbal, s_nationkey.n_name, ps_suppkey.s_name -- Q2_outer'

    ##  # TODO: this query originaly SELECTs min(partsupp.ps_supplycost). I need to add 'min' feature like 'max'.
    Q 'SELECT max(partsupp.ps_supplycost) FROM partsupp.ps_suppkey.s_nationkey.n_regionkey '\
      'WHERE n_regionkey.r_name = ? -- Q2_inner'

    Q 'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem.l_shipdate > ? '\
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'

    Q 'SELECT c_nationkey.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate >= ? AND l_orderkey.o_orderdate < ? ' \
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
      'GROUP BY c_nationkey.n_name -- Q5'

    Q 'SELECT c_nationkey.n_name, lineitem.l_shipdate, '\
            'sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
      'WHERE c_nationkey.n_name = ? '\
            'AND lineitem.l_shipdate < ? AND lineitem.l_shipdate > ? ' \
      'ORDER BY c_nationkey.n_name, lineitem.l_shipdate ' \
      'GROUP BY c_nationkey.n_name, lineitem.l_shipdate -- Q7'
  end

  Group 'Group2', default: frequencies do
    #Q 'INSERT INTO orders SET o_orderkey=?, o_orderstatus=?, o_totalprice=?, o_orderdate=?, o_orderpriority=?, '\
    #            'o_clerk=?, o_shippriority=?, o_comment=?, dummy=? AND CONNECT TO from_lineitem(?), o_custkey(?) -- orders_insert'


    Q 'SELECT l_orderkey.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
      'FROM part.from_partsupp.from_lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE c_nationkey.n_name = ? AND n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? AND l_orderkey.o_orderdate > ? AND part.p_type = ? ' \
      'ORDER BY l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderdate -- Q8'

    Q 'SELECT c_nationkey.n_name, l_orderkey.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount), '  \
          'sum(from_partsupp.ps_supplycost), sum(from_lineitem.l_quantity) ' \
      'FROM part.from_partsupp.from_lineitem.l_orderkey.o_custkey.c_nationkey ' \
      'WHERE part.p_name = ? AND l_orderkey.o_orderkey = ? ' \
      'ORDER BY c_nationkey.n_name, l_orderkey.o_orderdate ' \
      'GROUP BY c_nationkey.n_name, l_orderkey.o_orderdate -- Q9'

    Q 'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
          'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), '\
          'o_custkey.c_acctbal, c_nationkey.n_name, '\
          'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
       'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
       'WHERE l_orderkey.o_orderdate >= ? AND l_orderkey.o_orderdate < ? AND lineitem.l_returnflag = ? '\
       'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
       'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10'

    Q 'SELECT sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) ' \
      'FROM partsupp.ps_suppkey.s_nationkey '\
      'WHERE s_nationkey.n_name = ? AND partsupp.ps_supplycost = ? AND partsupp.ps_availqty = ? '\
      'ORDER BY partsupp.ps_supplycost, partsupp.ps_availqty ' \
      'GROUP BY partsupp.ps_partkey -- Q11_outer'

    Q 'SELECT sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) '\
      'FROM partsupp.ps_suppkey.s_nationkey '\
      'WHERE s_nationkey.n_name = ? -- Q11_inner'

    Q 'SELECT lineitem.l_shipmode, sum(l_orderkey.o_orderpriority) '\
      'FROM lineitem.l_orderkey '\
      'WHERE lineitem.l_shipmode = ? AND lineitem.l_commitdate < ? ' \
          'AND lineitem.l_commitdate > ? AND lineitem.l_shipdate < ? ' \
          'AND lineitem.l_receiptdate > ? AND lineitem.l_receiptdate >= ? AND lineitem.l_receiptdate < ? ' \
      'ORDER BY lineitem.l_shipmode ' \
      'GROUP BY lineitem.l_shipmode -- Q12'
  end
end
