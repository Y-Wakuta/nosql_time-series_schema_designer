# frozen_string_literal: true


NoSE::TimeDependWorkload.new do
  #Model 'tpch'
  #Model 'tpch_card'
  #Model 'tpch_card_key_composite'
  #Model 'tpch_card_key_composite_2_lineitems'
  Model 'tpch_card_key_composite_dup_lineitems_order_customer'

  def step_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    middle_ts = timesteps / 2
    (0..timesteps).map do |current_ts|
      current_ts <= middle_ts ? start_ratio : end_ratio
    end
  end

  step = step_freq(0.1, 0.9, 8)

  frequencies = step

  TimeSteps frequencies.size
  Interval 7200 # specify interval in minutes

  Group 'G1', default: frequencies do
      Q 'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND lineitem.l_shipdate > ? '\
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'

      Q 'SELECT c_nationkey.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? ' \
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
      'GROUP BY c_nationkey.n_name -- Q5'

      # Q 'SELECT c_nationkey.n_name, lineitem.l_shipdate, '\
      #       'sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      # 'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
      # 'WHERE c_nationkey.n_name = ? '\
      #       'AND lineitem.l_shipdate < ? ' \
      # 'ORDER BY c_nationkey.n_name, lineitem.l_shipdate ' \
      # 'GROUP BY c_nationkey.n_name, lineitem.l_shipdate -- Q7'
  end

  Group 'G2', default: frequencies.reverse do
        Q 'SELECT l_orderkey.o_orderkey, sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem_dup.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND lineitem_dup.l_shipdate > ? '\
      'ORDER BY lineitem_dup.l_extendedprice, lineitem_dup.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3-dup'

        Q 'SELECT c_nationkey.n_name, sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount) ' \
      'FROM lineitem_dup.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? ' \
      'ORDER BY lineitem_dup.l_extendedprice, lineitem_dup.l_discount ' \
      'GROUP BY c_nationkey.n_name -- Q5-dup'

        #   Q 'SELECT c_nationkey.n_name, lineitem_dup.l_shipdate, '\
     #       'sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount) ' \
     # 'FROM lineitem_dup.l_orderkey.o_custkey.c_nationkey '\
     # 'WHERE c_nationkey.n_name = ? '\
     #       'AND lineitem_dup.l_shipdate < ? ' \
     # 'ORDER BY c_nationkey.n_name, lineitem_dup.l_shipdate ' \
     # 'GROUP BY c_nationkey.n_name, lineitem_dup.l_shipdate -- Q7-dup'

  end
end
