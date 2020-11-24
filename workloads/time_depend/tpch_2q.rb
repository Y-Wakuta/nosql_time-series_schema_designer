# frozen_string_literal: true

NoSE::TimeDependWorkload.new do
  #Model 'tpch'
  Model 'tpch_card'

  #doubled = (0...12).map{|i| 2 ** i}
  doubled = (0...5).map{|i| 5 ** i}
  #doubled = (0...5).map{|i| 2 ** i}

  def linear_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    (0..timesteps).map do |current_ts|
      (((end_ratio - start_ratio) / timesteps) * current_ts + start_ratio).round(5)
    end
  end

  def step_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    middle_ts = timesteps / 2
    (0..timesteps).map do |current_ts|
      current_ts <= middle_ts ? start_ratio : end_ratio
    end
  end

  #step = step_freq(0.001, 0.999, 3)
  step = step_freq(0.001, 0.999, 4)
  linear = linear_freq(0.001, 0.999, 5)

  frequencies = step

  TimeSteps frequencies.size
  Interval 7200 # specify interval in minutes
  #Static true
  #FirstTs true
  #LastTs true
  average_latency = [(frequencies.reduce(:+).to_f / frequencies.size)] * frequencies.size

  Group 'Upseart', default: average_latency do
    Q 'INSERT INTO lineitem SET l_linenumber=?, l_quantity=?, l_extendedprice=?, l_discount=?, ' \
                  'l_tax = ?, l_returnflag=?, l_linestatus=?, l_shipdate=?, l_commitdate=?, l_receiptdate=?, ' \
                  'l_shipmode=?, l_comment=?, dummy=? AND CONNECT TO l_partkey(?), l_orderkey(?) -- lineitem_insert'
    Q 'INSERT INTO orders SET o_orderkey=?, o_orderstatus=?, o_totalprice=?, o_orderdate=?, o_orderpriority=?, '\
                'o_clerk=?, o_shippriority=?, o_comment=?, dummy=? AND CONNECT TO from_lineitem(?), o_custkey(?) -- orders_insert'
  end

  Group 'Group1', default: frequencies do
    Q 'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem.l_shipdate > ? '\
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'
  end

  Group 'Group2', default: frequencies.reverse do
     Q 'SELECT c_nationkey.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
       'FROM lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
       'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate >= ? AND l_orderkey.o_orderdate < ? ' \
       'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
       'GROUP BY c_nationkey.n_name -- Q5'

     Q 'SELECT l_orderkey.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
      'FROM part.from_partsupp.from_lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE c_nationkey.n_name = ? AND n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? AND l_orderkey.o_orderdate > ? AND part.p_type = ? ' \
      'ORDER BY l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderdate -- Q8'
  end
end
