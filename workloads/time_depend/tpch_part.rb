# frozen_string_literal: true


NoSE::TimeDependWorkload.new do
  Model 'tpch_part'

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

  Group 'OLAP', default: frequencies.reverse do
    Q 'SELECT ps_suppkey.s_acctbal, ps_suppkey.s_name, part.p_partkey, part.p_mfgr, '\
         'ps_suppkey.s_address, ps_suppkey.s_phone, ps_suppkey.s_comment ' \
       'FROM part.from_partsupp.ps_suppkey ' \
       'WHERE part.p_size = ? AND part.p_type = ? AND from_partsupp.ps_supplycost = ? -- part1'

    Q 'SELECT from_lineitem.l_extendedprice, from_lineitem.l_discount '\
      'FROM part.from_partsupp.from_lineitem ' \
      'WHERE part.p_type = ? -- part2'

    Q 'SELECT from_lineitem.l_extendedprice, from_lineitem.l_discount, '  \
          'from_partsupp.ps_supplycost, from_lineitem.l_quantity ' \
      'FROM part.from_partsupp.from_lineitem ' \
      'WHERE part.p_name = ? -- part3'
  end
end

