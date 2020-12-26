# frozen_string_literal: true

#ts = 3
#NoSE::TimeDependWorkload.new do
#  TimeSteps ts
#  Interval 4000
#  Model 'rubis'
#
#  Group 'Test1', 1.0, default: (0...ts).map{|i| 10 ** i} do
#    Q 'SELECT users.* FROM users WHERE users.id = ? -- 0'
#    Q 'SELECT users.* FROM users WHERE users.rating=? -- 1'
#    Q 'INSERT INTO users SET id = ?, rating = ? -- 2'
#  end
#
#  Group 'Test2', 1.0, default: (0...ts).map{|i| 10 ** i}.reverse do
#    Q 'SELECT items.* FROM items WHERE items.id = ? -- 3'
#    Q 'SELECT items.* FROM items WHERE items.quantity=? -- 4'
#    Q 'INSERT INTO items SET id = ?, quantity = ? -- 5.size'
#  end
#end

ts = 4
NoSE::TimeDependWorkload.new do
  TimeSteps ts
  Interval 4000
  Model 'tpch_card'
  doubled = (0...ts).map{|i| 100 ** i}

  Group 'Upseart', default: doubled.reverse do
    Q 'INSERT INTO lineitem SET l_orderkey=?, l_linenumber=?, l_quantity=?, l_extendedprice=?, l_discount=?, ' \
                  'l_tax = ?, l_returnflag=?, l_linestatus=?, l_shipdate=?, l_commitdate=?, l_receiptdate=?, ' \
                  'l_shipmode=?, l_comment=? AND CONNECT TO l_partkey(?), l_orderkey(?) -- 1'
    Q 'INSERT INTO orders SET o_orderkey=?, o_orderstatus=?, o_totalprice=?, o_orderdate=?, o_orderpriority=?, '\
                  'o_clerk=?, o_shippriority=?, o_comment=? AND CONNECT TO from_lineitem(?), o_custkey(?) -- 4'
  end

  Group 'Group1', default: doubled do
    Q 'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem.l_shipdate > ? '\
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'

    Q 'SELECT supplier.s_suppkey, supplier.s_name, supplier.s_address, supplier.s_phone, '\
          ' max(from_lineitem.l_extendedprice), max(from_lineitem.l_discount) ' \
      'FROM supplier.from_partsupp.from_lineitem.l_orderkey.o_custkey '\
      'WHERE supplier.s_suppkey = ? AND supplier.s_suppkey = ? AND from_lineitem.l_extendedprice = ? ' \
             ' AND from_lineitem.l_discount = ? AND from_lineitem.l_shipdate >= ? AND from_lineitem.l_shipdate < ? ' \
      'GROUP BY from_lineitem.l_partkey -- Q15'
  end
end

