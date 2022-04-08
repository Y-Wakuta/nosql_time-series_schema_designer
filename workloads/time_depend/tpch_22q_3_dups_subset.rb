# frozen_string_literal: true

NoSE::TimeDependWorkload.new do
  Model 'tpch_card_key_composite_dup_lineitems_order_customer'

  step_width = 3
  step_cyclic = [0.1] * step_width + [0.9] * step_width + [0.1] * step_width + [0.9] * step_width
  step_cyclic_revese = step_cyclic.map{|sc| (1.0 - sc).round(4)}

  TimeSteps step_cyclic.size
  Interval 7200 # specify interval in minutes

  Group 'group-lineitem', default: step_cyclic do

    Q 'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem.l_shipdate > ? '\
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'

    Q 'SELECT c_nationkey.n_name, lineitem.l_shipdate, '\
            'sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
      'WHERE c_nationkey.n_name = ? '\
            'AND lineitem.l_shipdate < ? ' \
      'ORDER BY c_nationkey.n_name, lineitem.l_shipdate ' \
      'GROUP BY c_nationkey.n_name, lineitem.l_shipdate -- Q7'

    Q 'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
          'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), '\
          'o_custkey.c_acctbal, c_nationkey.n_name, '\
          'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
       'FROM lineitem.l_orderkey.o_custkey.c_nationkey '\
       'WHERE lineitem.l_returnflag = ? AND l_orderkey.o_orderdate < ?  '\
       'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
       'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10'

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
  end

  Group 'group-lineitem-dup', default: step_cyclic_revese do
    Q 'SELECT l_orderkey.o_orderkey, sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
    'FROM lineitem_dup.l_orderkey.o_custkey '\
    'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem_dup.l_shipdate > ? '\
    'ORDER BY lineitem_dup.l_extendedprice, lineitem_dup.l_discount, l_orderkey.o_orderdate ' \
    'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3-dup'

    Q 'SELECT c_nationkey.n_name, lineitem_dup.l_shipdate, '\
          'sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount) ' \
    'FROM lineitem_dup.l_orderkey.o_custkey.c_nationkey '\
    'WHERE c_nationkey.n_name = ? '\
          'AND lineitem_dup.l_shipdate < ? ' \
    'ORDER BY c_nationkey.n_name, lineitem_dup.l_shipdate ' \
    'GROUP BY c_nationkey.n_name, lineitem_dup.l_shipdate -- Q7-dup'

    Q 'SELECT o_custkey.c_custkey, o_custkey.c_name, '\
        'sum(lineitem_dup.l_extendedprice), sum(lineitem_dup.l_discount), '\
        'o_custkey.c_acctbal, c_nationkey.n_name, '\
        'o_custkey.c_address, o_custkey.c_phone, o_custkey.c_comment '\
     'FROM lineitem_dup.l_orderkey.o_custkey.c_nationkey '\
     'WHERE lineitem_dup.l_returnflag = ? AND l_orderkey.o_orderdate < ?  '\
     'ORDER BY lineitem_dup.l_extendedprice, lineitem_dup.l_discount ' \
     'GROUP BY o_custkey.c_custkey, o_custkey.c_name, o_custkey.c_acctbal, o_custkey.c_phone, c_nationkey.n_name, o_custkey.c_address, o_custkey.c_comment -- Q10-dup'

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
  end
end
