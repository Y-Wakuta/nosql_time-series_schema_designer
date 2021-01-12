# frozen_string_literal: true


NoSE::TimeDependWorkload.new do
  #Model 'tpch'
  Model 'tpch_card'

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

  step = step_freq(0.001, 0.999, 6)
  linear = linear_freq(0.001, 0.999, 5)

  frequencies = step

  TimeSteps frequencies.size
  Interval 7200 # specify interval in minutes
  #Static true
  #FirstTs true
  #LastTs true

  Group 'Group1', default: frequencies.reverse do
    # Q 'SELECT sum(ps_partkey.p_type), sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
    #   'FROM orders.from_lineitem.l_partkey.ps_partkey '\
    #   'WHERE orders.o_orderkey = ? AND from_lineitem.l_shipdate >= ? AND from_lineitem.l_shipdate < ? -- Q14'
    #
    Q 'SELECT ps_partkey.p_size '\
       'FROM partsupp.ps_partkey '\
       'WHERE partsupp.ps_availqty = ? -- Q14'
    #Q 'SELECT part.p_size '\
    #   'FROM part.from_partsupp '\
    #   'WHERE from_partsupp.ps_availqty = ? -- Q14'
  end
end

