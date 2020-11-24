# frozen_string_literal: true


NoSE::TimeDependWorkload.new do
  Model 'tpch_card'

  def linear_freq(start_ratio, end_ratio, timesteps, current_ts)
    (((end_ratio - start_ratio) / timesteps) * current_ts + start_ratio).round(5)
  end

  linear = (0..4).map{|t| linear_freq(0.001, 0.999, 4, t)}

  TimeSteps linear.size
  Interval 7200 # specify interval in minutes

  Group 'Group1', default: linear.reverse do

    Q 'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
          'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), '\
          'o_custkey.c_acctbal, c_nationkey.n_name, '\
          'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
       'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
       'WHERE l_orderkey.o_orderdate >= ? AND l_orderkey.o_orderdate < ? AND lineitem.l_returnflag = ? '\
       'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
       'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10'
  end
end
