# frozen_string_literal: true

NoSE::TimeDependWorkload.new do
  Model 'tpch_card_key_composite_dup_lineitems_order_customer'

  def step_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    middle_ts = timesteps / 2
    (0..timesteps).map do |current_ts|
      current_ts <= middle_ts ? start_ratio : end_ratio
    end
  end

  step_width = 3
  step_cyclic = [0.1] * step_width + [0.9] * step_width + [0.1] * step_width + [0.9] * step_width
  step_cyclic_revese = step_cyclic.map{|sc| (1.0 - sc).round(4)}

  #frequencies = step

  #TimeSteps frequencies.size
  TimeSteps step_cyclic.size
  Interval 7200 # specify interval in minutes
  #Static true
  #FirstTs true
  #LastTs true

  Group 'group-lineitem', default: step_cyclic do
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
      'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? ' \
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
      'GROUP BY c_nationkey.n_name -- Q5'

    Q 'SELECT c_nationkey.n_name, lineitem.l_shipdate, '\
            'sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
      'WHERE c_nationkey.n_name = ? '\
            'AND lineitem.l_shipdate < ? ' \
      'ORDER BY c_nationkey.n_name, lineitem.l_shipdate ' \
      'GROUP BY c_nationkey.n_name, lineitem.l_shipdate -- Q7'

    Q 'SELECT l_orderkey.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
      'FROM part.from_partsupp.from_lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE n_regionkey.r_name = ? AND part.p_type = ? AND l_orderkey.o_orderdate < ? ' \
      'ORDER BY l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderdate -- Q8'

    Q 'SELECT c_nationkey.n_name, l_orderkey.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount), '  \
          'sum(from_partsupp.ps_supplycost), sum(from_lineitem.l_quantity) ' \
      'FROM part.from_partsupp.from_lineitem.l_orderkey.o_custkey.c_nationkey ' \
      'WHERE part.p_name = ? ' \
      'ORDER BY c_nationkey.n_name, l_orderkey.o_orderdate ' \
      'GROUP BY c_nationkey.n_name, l_orderkey.o_orderdate -- Q9'

    Q 'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
          'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), '\
          'o_custkey.c_acctbal, c_nationkey.n_name, '\
          'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
       'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
       'WHERE lineitem.l_returnflag = ? AND l_orderkey.o_orderdate < ?  '\
       'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
       'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10'

    Q 'SELECT sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) ' \
      'FROM partsupp.ps_suppkey.s_nationkey '\
      'WHERE s_nationkey.n_name = ? AND partsupp.ps_supplycost > ? AND partsupp.ps_availqty > ? '\
      'ORDER BY partsupp.ps_supplycost, partsupp.ps_availqty ' \
      'GROUP BY partsupp.ps_partkey -- Q11_outer'

    Q 'SELECT sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) '\
      'FROM partsupp.ps_suppkey.s_nationkey '\
      'WHERE s_nationkey.n_name = ? -- Q11_inner'

    # case が sum() に包含されていたので，そこで使用されたいた l_orderkey.o_orderpriority をそのまま sum に追加した
    Q 'SELECT lineitem.l_shipmode, count(l_orderkey.o_orderpriority) '\
      'FROM lineitem.l_orderkey '\
      'WHERE lineitem.l_shipmode = ? AND lineitem.l_receiptdate < ? ' \
      'ORDER BY lineitem.l_shipmode ' \
      'GROUP BY lineitem.l_shipmode -- Q12'

    Q 'SELECT o_custkey.c_custkey, count(orders.o_orderkey) ' \
      'FROM orders.o_custkey ' \
      'WHERE orders.o_comment = ? ' \
      'GROUP BY o_custkey.c_custkey, orders.o_orderkey -- Q13'

    #Q 'SELECT ps_partkey.p_type, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) '\
    #  'FROM lineitem.l_partkey.ps_partkey '\
    #  'WHERE ps_partkey.p_type = ? AND lineitem.l_shipdate < ? -- Q14'

    Q 'SELECT supplier.s_suppkey FROM supplier WHERE supplier.s_comment = ? -- Q16_inner'
    Q 'SELECT ps_partkey.p_brand, ps_partkey.p_type, ps_partkey.p_size, count(supplier.s_suppkey) ' \
       'FROM supplier.from_partsupp.ps_partkey ' \
       'WHERE ps_partkey.p_brand = ? AND ps_partkey.p_type = ? AND ps_partkey.p_size = ? AND supplier.s_suppkey = ? ' \
       'ORDER BY ps_partkey.p_brand, ps_partkey.p_type, ps_partkey.p_size ' \
       'GROUP BY ps_partkey.p_brand, ps_partkey.p_type, ps_partkey.p_size -- Q16_outer'

    Q 'SELECT sum(lineitem.l_extendedprice) ' \
      'FROM lineitem.l_partkey.ps_partkey ' \
      'WHERE ps_partkey.p_brand = ? AND ps_partkey.p_container = ? AND lineitem.l_quantity < ? -- Q17'

    Q 'SELECT o_custkey.c_name, o_custkey.c_custkey, l_orderkey.o_orderkey, ' \
      'l_orderkey.o_orderdate, l_orderkey.o_totalprice, sum(lineitem.l_quantity) ' \
      'FROM lineitem.l_orderkey.o_custkey ' \
      'WHERE l_orderkey.o_orderkey = ? ' \
      'ORDER BY l_orderkey.o_totalprice, l_orderkey.o_orderdate ' \
      'GROUP BY o_custkey.c_name, o_custkey.c_custkey, l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_totalprice -- Q18_outer'

    Q 'SELECT sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem.l_partkey.ps_partkey ' \
      'WHERE ps_partkey.p_brand = ? AND ps_partkey.p_container = ? AND lineitem.l_shipmode = ? AND lineitem.l_shipinstruct = ? ' \
      'AND ps_partkey.p_size > ? AND lineitem.l_quantity < ? -- Q19'

    Q 'SELECT part.p_partkey FROM part WHERE part.p_name = ? -- Q20_inner_inner_1'
    Q 'SELECT partsupp.ps_suppkey FROM partsupp WHERE partsupp.ps_partkey = ? AND partsupp.ps_availqty > ? -- Q20_inner'
    Q 'SELECT supplier.s_name, supplier.s_address ' \
      'FROM supplier.s_nationkey ' \
      'WHERE supplier.s_suppkey = ? AND s_nationkey.n_name = ? ' \
      'ORDER BY supplier.s_name -- Q20'

    Q 'SELECT ps_suppkey.s_name, count(orders.o_orderkey) ' \
      'FROM orders.from_lineitem.l_partkey.ps_suppkey.s_nationkey ' \
      'WHERE orders.o_orderstatus = ? AND s_nationkey.n_name = ? '\
      'ORDER BY ps_suppkey.s_name ' \
      'GROUP BY ps_suppkey.s_name -- Q21'

    Q 'SELECT avg(customer.c_acctbal) FROM customer ' \
      'WHERE customer.c_phone = ? AND customer.c_acctbal > ? -- Q22_inner_inner'
    Q 'SELECT customer.c_phone, sum(customer.c_acctbal), count(customer.c_custkey) ' \
      'FROM customer ' \
      'WHERE customer.c_phone = ? AND customer.c_acctbal > ? ' \
      'ORDER BY customer.c_phone ' \
      'GROUP BY customer.c_phone -- Q22'
  end

  Group 'group-lineitem-dup', default: step_cyclic_revese do
    Q 'SELECT ps_suppkey.s_acctbal, ps_suppkey.s_name, s_nationkey.n_name, part.p_partkey, part.p_mfgr, '\
         'ps_suppkey.s_address, ps_suppkey.s_phone, ps_suppkey.s_comment ' \
       'FROM part.from_partsupp.ps_suppkey.s_nationkey.n_regionkey ' \
       'WHERE part.p_size = ? AND part.p_type = ? AND n_regionkey.r_name = ? AND from_partsupp.ps_supplycost = ? '\
       'ORDER BY ps_suppkey.s_acctbal, s_nationkey.n_name, ps_suppkey.s_name -- Q2_outer-dup'

    ##  # TODO: this query originaly SELECTs min(partsupp.ps_supplycost). I need to add 'min' feature like 'max'.
    Q 'SELECT max(partsupp.ps_supplycost) FROM partsupp.ps_suppkey.s_nationkey.n_regionkey '\
      'WHERE n_regionkey.r_name = ? -- Q2_inner-dup'

    Q 'SELECT l_orderkey.o_orderkey, sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem_dup.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem_dup.l_shipdate > ? '\
      'ORDER BY lineitem_dup.l_extendedprice, lineitem_dup.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3-dup'

    Q 'SELECT c_nationkey.n_name, sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount) ' \
      'FROM lineitem_dup.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? ' \
      'ORDER BY lineitem_dup.l_extendedprice, lineitem_dup.l_discount ' \
      'GROUP BY c_nationkey.n_name -- Q5-dup'

    Q 'SELECT c_nationkey.n_name, lineitem_dup.l_shipdate, '\
            'sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount) ' \
      'FROM lineitem_dup.l_orderkey.o_custkey.c_nationkey '\
      'WHERE c_nationkey.n_name = ? '\
            'AND lineitem_dup.l_shipdate < ? ' \
      'ORDER BY c_nationkey.n_name, lineitem_dup.l_shipdate ' \
      'GROUP BY c_nationkey.n_name, lineitem_dup.l_shipdate -- Q7-dup'

    Q 'SELECT l_orderkey.o_orderdate, sum(from_lineitem_dup.l_extendedprice), sum(from_lineitem_dup.l_discount) '\
      'FROM part.from_partsupp.from_lineitem_dup.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
      'WHERE n_regionkey.r_name = ? AND part.p_type = ? AND l_orderkey.o_orderdate < ? ' \
      'ORDER BY l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderdate -- Q8-dup'

    Q 'SELECT c_nationkey.n_name, l_orderkey.o_orderdate, sum(from_lineitem_dup.l_extendedprice), sum(from_lineitem_dup.l_discount), '  \
          'sum(from_partsupp.ps_supplycost), sum(from_lineitem_dup.l_quantity) ' \
      'FROM part.from_partsupp.from_lineitem_dup.l_orderkey.o_custkey.c_nationkey ' \
      'WHERE part.p_name = ? ' \
      'ORDER BY c_nationkey.n_name, l_orderkey.o_orderdate ' \
      'GROUP BY c_nationkey.n_name, l_orderkey.o_orderdate -- Q9-dup'

    Q 'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
          'sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount), '\
          'o_custkey.c_acctbal, c_nationkey.n_name, '\
          'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
       'FROM lineitem_dup.l_orderkey.o_custkey.c_nationkey '\
       'WHERE lineitem_dup.l_returnflag = ? AND l_orderkey.o_orderdate < ?  '\
       'ORDER BY lineitem_dup.l_extendedprice, lineitem_dup.l_discount ' \
       'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10-dup'

    Q 'SELECT sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) ' \
      'FROM partsupp.ps_suppkey.s_nationkey '\
      'WHERE s_nationkey.n_name = ? AND partsupp.ps_supplycost > ? AND partsupp.ps_availqty > ? '\
      'ORDER BY partsupp.ps_supplycost, partsupp.ps_availqty ' \
      'GROUP BY partsupp.ps_partkey -- Q11_outer-dup'

    Q 'SELECT sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) '\
      'FROM partsupp.ps_suppkey.s_nationkey '\
      'WHERE s_nationkey.n_name = ? -- Q11_inner-dup'

    # case が sum() に包含されていたので，そこで使用されたいた l_orderkey.o_orderpriority をそのまま sum に追加した．しかし，sum は string の属性に使えないので，count に変更した
    Q 'SELECT lineitem_dup.l_shipmode, count(l_orderkey.o_orderpriority) '\
      'FROM lineitem_dup.l_orderkey '\
      'WHERE lineitem_dup.l_shipmode = ? AND lineitem_dup.l_receiptdate < ? ' \
      'ORDER BY lineitem_dup.l_shipmode ' \
      'GROUP BY lineitem_dup.l_shipmode -- Q12-dup'

    Q 'SELECT o_custkey.c_custkey, count(orders_dup.o_orderkey) ' \
      'FROM orders_dup.o_custkey ' \
      'WHERE orders_dup.o_comment = ? ' \
      'GROUP BY o_custkey.c_custkey, orders_dup.o_orderkey -- Q13-dup'

    #Q 'SELECT ps_partkey.p_type, sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount) '\
    #  'FROM lineitem_dup.l_partkey.ps_partkey '\
    #  'WHERE ps_partkey.p_type = ? AND lineitem_dup.l_shipdate < ? -- Q14-dup'

    Q 'SELECT supplier.s_suppkey FROM supplier WHERE supplier.s_comment = ? -- Q16_inner-dup'
    Q 'SELECT ps_partkey.p_brand, ps_partkey.p_type, ps_partkey.p_size, count(supplier.s_suppkey) ' \
       'FROM supplier.from_partsupp.ps_partkey ' \
       'WHERE ps_partkey.p_brand = ? AND ps_partkey.p_type = ? AND ps_partkey.p_size = ? AND supplier.s_suppkey = ? ' \
       'ORDER BY ps_partkey.p_brand, ps_partkey.p_type, ps_partkey.p_size ' \
       'GROUP BY ps_partkey.p_brand, ps_partkey.p_type, ps_partkey.p_size -- Q16_outer-dup'

    Q 'SELECT sum(lineitem_dup.l_extendedprice) ' \
      'FROM lineitem_dup.l_partkey.ps_partkey ' \
      'WHERE ps_partkey.p_brand = ? AND ps_partkey.p_container = ? AND lineitem_dup.l_quantity < ? -- Q17-dup'

    Q 'SELECT o_custkey.c_name, o_custkey.c_custkey, l_orderkey.o_orderkey, ' \
      'l_orderkey.o_orderdate, l_orderkey.o_totalprice, sum(lineitem_dup.l_quantity) ' \
      'FROM lineitem_dup.l_orderkey.o_custkey ' \
      'WHERE l_orderkey.o_orderkey = ? ' \
      'ORDER BY l_orderkey.o_totalprice, l_orderkey.o_orderdate ' \
      'GROUP BY o_custkey.c_name, o_custkey.c_custkey, l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_totalprice -- Q18_outer-dup'

    Q 'SELECT sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount) ' \
      'FROM lineitem_dup.l_partkey.ps_partkey ' \
      'WHERE ps_partkey.p_brand = ? AND ps_partkey.p_container = ? AND lineitem_dup.l_shipmode = ? AND lineitem_dup.l_shipinstruct = ? ' \
      'AND ps_partkey.p_size > ? AND lineitem_dup.l_quantity < ? -- Q19-dup'

    Q 'SELECT part.p_partkey FROM part WHERE part.p_name = ? -- Q20_inner_inner_1-dup'
    Q 'SELECT partsupp.ps_suppkey FROM partsupp WHERE partsupp.ps_partkey = ? AND partsupp.ps_availqty > ? -- Q20_inner-dup'
    Q 'SELECT supplier.s_name, supplier.s_address ' \
      'FROM supplier.s_nationkey ' \
      'WHERE supplier.s_suppkey = ? AND s_nationkey.n_name = ? ' \
      'ORDER BY supplier.s_name -- Q20-dup'

    Q 'SELECT ps_suppkey.s_name, count(orders_dup.o_orderkey) ' \
      'FROM orders_dup.from_lineitem_dup.l_partkey.ps_suppkey.s_nationkey ' \
      'WHERE orders_dup.o_orderstatus = ? AND s_nationkey.n_name = ? '\
      'ORDER BY ps_suppkey.s_name ' \
      'GROUP BY ps_suppkey.s_name -- Q21-dup'

    Q 'SELECT avg(customer_dup.c_acctbal) FROM customer_dup ' \
      'WHERE customer_dup.c_phone = ? AND customer_dup.c_acctbal > ? -- Q22_inner_inner-dup'
    Q 'SELECT customer_dup.c_phone, sum(customer_dup.c_acctbal), count(customer_dup.c_custkey) ' \
      'FROM customer_dup ' \
      'WHERE customer_dup.c_phone = ? AND customer_dup.c_acctbal > ? ' \
      'ORDER BY customer_dup.c_phone ' \
      'GROUP BY customer_dup.c_phone -- Q22-dup'
  end
end
