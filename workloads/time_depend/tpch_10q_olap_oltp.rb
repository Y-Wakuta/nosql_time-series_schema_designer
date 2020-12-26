# frozen_string_literal: true


NoSE::TimeDependWorkload.new do
  #Model 'tpch'
  Model 'tpch_card'

  #doubled = (0...12).map{|i| 2 ** i}
  #doubled = (0...5).map{|i| 10 ** i}
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

  step = step_freq(0.001, 0.999, 6)
  linear = linear_freq(0.001, 0.999, 5)

  frequencies = step

  TimeSteps frequencies.size
  Interval 7200 # specify interval in minutes
  #Static true
  #FirstTs true
  #LastTs true

  # ====================================================
  # OLAP, OLTP 分け方
  # 4つ以上の entity をジョインしている -> OLAP
  # それ以外のクエリ、更新処理 -> OLTP
  # ====================================================

  Group 'OLAP', default: frequencies.reverse do
    Q 'SELECT ps_suppkey.s_acctbal, ps_suppkey.s_name, s_nationkey.n_name, part.p_partkey, part.p_mfgr, '\
         'ps_suppkey.s_address, ps_suppkey.s_phone, ps_suppkey.s_comment ' \
       'FROM part.from_partsupp.ps_suppkey.s_nationkey.n_regionkey ' \
       'WHERE part.p_size = ? AND part.p_type = ? AND n_regionkey.r_name = ? AND from_partsupp.ps_supplycost = ? '\
       'ORDER BY ps_suppkey.s_acctbal, s_nationkey.n_name, ps_suppkey.s_name -- Q2_outer'

    ##  # TODO: this query originaly SELECTs min(partsupp.ps_supplycost). I need to add 'min' feature like 'max'.
    Q 'SELECT max(partsupp.ps_supplycost) FROM partsupp.ps_suppkey.s_nationkey.n_regionkey '\
      'WHERE n_regionkey.r_name = ? -- Q2_inner'

    # このクエリも Q4 と同様に１つしか eq predicate を持たないが，その条件は primary key 属性を使用している訳では無いし SELECT 句の中も region を参照していないのでこちらは多数のジョインプランが出せるはず
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
  end

  Group 'OLTP', default: frequencies do

    Q 'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
      'FROM lineitem.l_orderkey.o_custkey '\
      'WHERE o_custkey.c_mktsegment = ? AND l_orderkey.o_orderdate < ? AND lineitem.l_shipdate > ? '\
      'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
      'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'

    Q 'SELECT lineitem.l_returnflag, lineitem.l_linestatus, sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), '\
        'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), sum(lineitem.l_extendedprice), '\
        'sum(lineitem.l_discount), sum(lineitem.l_tax), avg(lineitem.l_quantity), '\
        'avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(l_orderkey.o_orderkey) '\
      'FROM lineitem.l_orderkey '\
      'WHERE lineitem.dummy = ? AND lineitem.l_shipdate <= ? ' \
      'GROUP BY lineitem.l_returnflag, lineitem.l_linestatus -- Q1'

    # this query only has one eq predicate and the predicates uses primary key field. Therefore, this query possibly does not use join plans
    Q 'SELECT l_orderkey.o_orderpriority, count(l_orderkey.o_orderkey) ' \
      'FROM lineitem.l_orderkey '\
      'WHERE l_orderkey.dummy = ? AND l_orderkey.o_orderdate >= ? AND l_orderkey.o_orderdate < ? AND lineitem.l_commitdate < ? AND lineitem.l_receiptdate > ? ' \
      'ORDER BY l_orderkey.o_orderpriority ' \
      'GROUP BY l_orderkey.o_orderpriority -- Q4'

    Q 'SELECT sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem ' \
      'WHERE lineitem.dummy = 1 AND lineitem.l_shipdate >= ? AND lineitem.l_shipdate < ? ' \
          'AND lineitem.l_discount > ? AND lineitem.l_discount < ? ' \
          'AND lineitem.l_quantity < ? -- Q6'

    Q 'INSERT INTO lineitem SET l_linenumber=?, l_quantity=?, l_extendedprice=?, l_discount=?, ' \
                  'l_tax = ?, l_returnflag=?, l_linestatus=?, l_shipdate=?, l_commitdate=?, l_receiptdate=?, ' \
                  'l_shipmode=?, l_comment=?, dummy=? AND CONNECT TO l_partkey(?), l_orderkey(?) -- lineitem_insert'
    Q 'INSERT INTO orders SET o_orderkey=?, o_orderstatus=?, o_totalprice=?, o_orderdate=?, o_orderpriority=?, '\
                'o_clerk=?, o_shippriority=?, o_comment=?, dummy=? AND CONNECT TO from_lineitem(?), o_custkey(?) -- orders_insert'
  end

  #  Group 'Group1', default: frequencies.reverse do
#    #    # === Q13 ===
#    #    #select
#    #    #   c_count, count(*) as custdist
#    #    #from (
#    #    #    select
#    #    #      c_custkey,
#    #    #      count(o_orderkey)
#    #    #    from
#    #    #      customer left outer join orders on
#    #    #      c_custkey = o_custkey
#    #    #      and o_comment not like ‘%[WORD1]%[WORD2]%’
#    #    #    group by
#    #    #      c_custkey
#    #    #  )as c_orders (c_custkey, c_count)
#    #    #group by
#    #    #   c_count
#    #    #order by
#    #    #   custdist desc,
#    #    #   c_count desc;
#    #    Q 'SELECT o_custkey.c_custkey, count(orders.o_orderkey) ' \
#    #      'FROM orders.o_custkey ' \
#    #      'WHERE orders.o_comment = ? ' \
#    #      'GROUP BY o_custkey.c_custkey, orders.o_orderkey -- Q13'
#    #
#    #    # === Q14 ===
#    #    #   select
#    #    #      100.00 * sum(case
#    #    #         when p_type like 'PROMO%'
#    #    #         then l_extendedprice*(1-l_discount)
#    #    #         else 0
#    #    #      end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
#    #    #   from
#    #    #      lineitem,
#    #    #      part
#    #    #   where
#    #    #      l_partkey = p_partkey
#    #    #      and l_shipdate >= date '[DATE]'
#    #    #      and l_shipdate < date '[DATE]' + interval '1' month;
#    #
#    #    # TODO: deal with composite key both of them are foreign key to other entity. currently deal with this problem by removing ps_partkey.p_type
#    #    #Q 'SELECT sum(ps_partkey.p_type), sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
#    #    #  'FROM orders.from_lineitem.l_partkey.ps_partkey '\
#    #    #  'WHERE orders.o_orderkey = ? AND from_lineitem.l_shipdate >= ? AND from_lineitem.l_shipdate < ? -- Q14'
#    #    Q 'SELECT sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
#    #      'FROM orders.from_lineitem '\
#    #      'WHERE orders.o_orderkey = ? AND from_lineitem.l_shipdate >= ? AND from_lineitem.l_shipdate < ? -- Q14'
#    #
#    #    # === Q15 ===
#    #    #   create view revenue[STREAM_ID] (supplier_no, total_revenue) as
#    #    #       select
#    #    #       l_suppkey,
#    #    #       sum(l_extendedprice * (1 - l_discount))
#    #    #       from
#    #    #       lineitem
#    #    #       where
#    #    #       l_shipdate >= date '[DATE]'
#    #    #       and l_shipdate < date '[DATE]' + interval '3' month
#    #    #       group by
#    #    #       l_suppkey;
#    #    #
#    #    #   select
#    #    #      s_suppkey,
#    #    #      s_name,
#    #    #      s_address,
#    #    #      s_phone,
#    #    #      total_revenue
#    #    #   from
#    #    #      supplier,
#    #    #      revenue[STREAM_ID]
#    #    #   where
#    #    #      s_suppkey = supplier_no
#    #    #      and total_revenue = (
#    #    #        select
#    #    #        max(total_revenue)
#    #    #        from
#    #    #        revenue[STREAM_ID]
#    #    #      )
#    #    #   order by
#    #    #      s_suppkey;
#    #    # Q 'SELECT to_supplier.s_suppkey, to_supplier.s_name, to_supplier.s_address, to_supplier.s_phone, '\
#    #    #       ' lineitem.l_suppkey, max(lineitem.l_extendedprice), max(lineitem.l_discount) ' \
#    #    #   'FROM lineitem.l_orderkey.o_custkey.to_supplier '\
#    #    #   'WHERE to_supplier.s_suppkey = ? AND lineitem.l_suppkey = ? AND lineitem.l_extendedprice = ? ' \
#    #    #          ' AND lineitem.l_discount = ? AND lineitem.l_shipdate >= ? AND lineitem.l_shipdate < ? ' \
#    #    #   'GROUP BY lineitem.l_suppkey -- Q15'
#    #   Q 'SELECT supplier.s_suppkey, supplier.s_name, supplier.s_address, supplier.s_phone, '\
#    #          ' max(from_lineitem.l_extendedprice), max(from_lineitem.l_discount) ' \
#    #      'FROM supplier.from_partsupp.from_lineitem.l_orderkey.o_custkey '\
#    #      'WHERE supplier.s_suppkey = ? AND supplier.s_suppkey = ? AND from_lineitem.l_extendedprice = ? ' \
#    #             ' AND from_lineitem.l_discount = ? AND from_lineitem.l_shipdate >= ? AND from_lineitem.l_shipdate < ? ' \
#    #      'GROUP BY from_lineitem.l_suppkey -- Q15'
#    #
#    #    # === Q16 ===
#    #    # select
#    #    #     p_brand,
#    #    #     p_type,
#    #    #     p_size,
#    #    #     count(distinct ps_suppkey) as supplier_cnt
#    #    # from
#    #    #     partsupp,
#    #    #     part
#    #    # where
#    #    #     p_partkey = ps_partkey
#    #    #     and p_brand <> '[BRAND]'
#    #    #     and p_type not like '[TYPE]%'
#    #    #     and p_size in ([SIZE1], [SIZE2], [SIZE3], [SIZE4], [SIZE5], [SIZE6], [SIZE7], [SIZE8])
#    #    #     and ps_suppkey not in (
#    #    #         select
#    #    #         s_suppkey
#    #    #         from
#    #    #         supplier
#    #    #         where
#    #    #         s_comment like '%Customer%Complaints%'
#    #    #     )
#    #    # group by
#    #    #     p_brand,
#    #    #     p_type,
#    #    #     p_size
#    #    # order by
#    #    #     supplier_cnt desc,
#    #    #     p_brand,
#    #    #     p_type,
#    #    #     p_size;
#    #    #    Q 'SELECT supplier.s_suppkey FROM supplier WHERE supplier.comment = ? -- Q16_inner'
#    #    #    Q 'SELECT to_part.p_brand, to_part.p_type, to_part.p_size, count(partsupp.ps_suppkey) ' \
#    #    #      'FROM partsupp.to_part ' \
#    #    #      'WHERE to_part.p_brand = ? AND to_part.p_type = ? AND to_part.p_size = ? AND partsupp.ps_suppkey = ? ' \
#    #    #      'ORDER BY to_part.p_brand, to_part.p_type, to_part.p_size ' \
#    #    #      'GROUP BY to_part.p_brand, to_part.p_type, to_part.p_size -- Q16_outer'
#    #    #
#    #    #    # with ID field orderby
#    #    #    #Q 'SELECT to_part.p_brand, to_part.p_type, to_part.p_size, count(partsupp.ps_suppkey) ' \
#    #    #    #  'FROM partsupp.to_part ' \
#    #    #    #  'WHERE to_part.p_brand = ? AND to_part.p_type = ? AND to_part.p_size = ? AND partsupp.ps_suppkey = ? ' \
#    #    #    #  'ORDER BY partsupp.ps_suppkey, to_part.p_brand, to_part.p_type, to_part.p_size ' \
#    #    #    #  'GROUP BY to_part.p_brand, to_part.p_type, to_part.p_size -- Q16_outer'
#    #    #
#    #    #    # == Q17 ==
#    #    #    # select
#    #    #    #   sum(l_extendedprice) / 7.0 as avg_yearly
#    #    #    # from
#    #    #    #   lineitem,
#    #    #    #   part
#    #    #    # where
#    #    #    #   p_partkey = l_partkey
#    #    #    #   and p_brand = '[BRAND]'
#    #    #    #   and p_container = '[CONTAINER]'
#    #    #    #   and l_quantity < (
#    #    #    #     select
#    #    #    #     0.2 * avg(l_quantity)
#    #    #    #     from
#    #    #    #     lineitem
#    #    #    #     where
#    #    #    #     l_partkey = p_partkey
#    #    #    #   );
#    #    #    Q 'SELECT sum(lineitem.l_extendedprice) ' \
#    #    #      'FROM lineitem.l_partkey.to_part ' \
#    #    #      'WHERE to_part.p_brand = ? AND to_part.p_container = ? AND lineitem.l_quantity < ? -- Q17'
#    #    #
#    #    #    # == Q18 ==
#    #    #    #select
#    #    #    #   c_name,
#    #    #    #   c_custkey,
#    #    #    #   o_orderkey,
#    #    #    #   o_orderdate,
#    #    #    #   o_totalprice,
#    #    #    #   sum(l_quantity)
#    #    #    #from
#    #    #    #   customer,
#    #    #    #   orders,
#    #    #    #   lineitem
#    #    #    #where
#    #    #    #   o_orderkey in (
#    #    #    #       select
#    #    #    #       l_orderkey
#    #    #    #       from
#    #    #    #       lineitem
#    #    #    #       group by
#    #    #    #       l_orderkey having
#    #    #    #       sum(l_quantity) > [QUANTITY]
#    #    #    #    )
#    #    #    #   and c_custkey = o_custkey
#    #    #    #   and o_orderkey = l_orderkey
#    #    #    # group by
#    #    #    #   c_name,
#    #    #    #   c_custkey,
#    #    #    #   o_orderkey,
#    #    #    #   o_orderdate,
#    #    #    #   o_totalprice
#    #    #    # order by
#    #    #    #   o_totalprice desc,
#    #    #    #   o_orderdate;
#    #    #         Q 'SELECT lineitem.l_orderkey FROM lineitem ' \
#    #    #           'WHERE lineitem.dummy = ? AND lineitem.l_quantity > ? ' \
#    #    #           'GROUP BY lineitem.l_orderkey -- Q18_inner'
#    #    #         Q 'SELECT o_custkey.c_name, o_custkey.c_custkey, l_orderkey.o_orderkey, ' \
#    #    #           'l_orderkey.o_orderdate, l_orderkey.o_totalprice, sum(lineitem.l_quantity) ' \
#    #    #           'FROM lineitem.l_orderkey.o_custkey ' \
#    #    #           'WHERE l_orderkey.o_orderkey = ? ' \
#    #    #           'ORDER BY l_orderkey.o_totalprice, l_orderkey.o_orderdate ' \
#    #    #           'GROUP BY o_custkey.c_custkey, l_orderkey.o_orderkey -- Q18_outer'
#    #    #
#    #    #    # == Q19 ==
#    #    #    #select
#    #    #    #   sum(l_extendedprice * (1 - l_discount) ) as revenue
#    #    #    #from
#    #    #    #   lineitem,
#    #    #    #   part
#    #    #    #where
#    #    #    #   (
#    #    #    #     p_partkey = l_partkey
#    #    #    #     and p_brand = ‘[BRAND1]’
#    #    #    #     and p_container in ( ‘SM CASE’, ‘SM BOX’, ‘SM PACK’, ‘SM PKG’)
#    #    #    #     and l_quantity >= [QUANTITY1] and l_quantity <= [QUANTITY1] + 10
#    #    #    #     and p_size between 1 and 5
#    #    #    #     and l_shipmode in (‘AIR’, ‘AIR REG’)
#    #    #    #     and l_shipinstruct = ‘DELIVER IN PERSON’
#    #    #    #   )
#    #    #    #   or
#    #    #    #   (
#    #    #    #     p_partkey = l_partkey
#    #    #    #     and p_brand = ‘[BRAND2]’
#    #    #    #     and p_container in (‘MED BAG’, ‘MED BOX’, ‘MED PKG’, ‘MED PACK’)
#    #    #    #     and l_quantity >= [QUANTITY2] and l_quantity <= [QUANTITY2] + 10
#    #    #    #     and p_size between 1 and 10
#    #    #    #     and l_shipmode in (‘AIR’, ‘AIR REG’)
#    #    #    #     and l_shipinstruct = ‘DELIVER IN PERSON’
#    #    #    #   )
#    #    #    #   or
#    #    #    #   (
#    #    #    #     p_partkey = l_partkey
#    #    #    #     and p_brand = ‘[BRAND3]’
#    #    #    #     and p_container in ( ‘LG CASE’, ‘LG BOX’, ‘LG PACK’, ‘LG PKG’)
#    #    #    #     and l_quantity >= [QUANTITY3] and l_quantity <= [QUANTITY3] + 10
#    #    #    #     and p_size between 1 and 15
#    #    #    #     and l_shipmode in (‘AIR’, ‘AIR REG’)
#    #    #    #     and l_shipinstruct = ‘DELIVER IN PERSON’
#    #    #    #   );
#    #    #         Q 'SELECT sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
#    #    #           'FROM lineitem.l_partkey.to_part ' \
#    #    #           'WHERE to_part.p_brand = ? AND to_part.p_container = ? AND lineitem.l_quantity > ? ' \
#    #    #           'AND to_part.p_size > ? AND lineitem.l_shipdate = ? AND lineitem.l_shipinstruct = ? -- Q19'
#    #    #
#    #    #    # == Q20 ==
#    #    #    # select
#    #    #    #   s_name,
#    #    #    #   s_address
#    #    #    # from
#    #    #    #   supplier, nation
#    #    #    # where
#    #    #    #   s_suppkey in (
#    #    #    #     select
#    #    #    #       ps_suppkey
#    #    #    #     from
#    #    #    #       partsupp
#    #    #    #     where
#    #    #    #       ps_partkey in (
#    #    #    #         select
#    #    #    #           p_partkey
#    #    #    #         from
#    #    #    #           part
#    #    #    #         where
#    #    #    #           p_name like '[COLOR]%'
#    #    #    #       )
#    #    #    #       and ps_availqty > (
#    #    #    #         select
#    #    #    #           0.5 * sum(l_quantity)
#    #    #    #         from
#    #    #      #         lineitem
#    #    #    #         where
#    #    #    #           l_partkey = ps_partkey
#    #    #    #           and l_suppkey = ps_suppkey
#    #    #    #           and l_shipdate >= date('[DATE]’)
#    #    #    #           and l_shipdate < date('[DATE]’) + interval ‘1’ year
#    #    #    #       )
#    #    #    #   )
#    #    #    #   and s_nationkey = n_nationkey
#    #    #    #   and n_name = '[NATION]'
#    #    #    # order by
#    #    #    #   s_name;
#    #    #         Q 'SELECT part.p_partkey FROM part WHERE part.p_name = ? -- Q20_inner_inner_1'
#    #    #         Q 'SELECT sum(lineitem.l_quantity) FROM lineitem WHERE lineitem.dummy = ? AND lineitem.l_shipdate > ? -- Q20_inner_inner_2'
#    #    #         Q 'SELECT partsupp.ps_suppkey FROM partsupp WHERE partsupp.ps_partkey = ? AND partsupp.ps_availqty > ? -- Q20_inner'
#    #    #         Q 'SELECT supplier.s_name, supplier.s_address ' \
#    #    #           'FROM supplier.s_nationkey ' \
#    #    #           'WHERE supplier.s_suppkey = ? AND to_nation.n_name = ? ' \
#    #    #           'ORDER BY supplier.s_name -- Q20'
#    #    #
#    #    #    # == Q21 ==
#    #    #    # select
#    #    #    #   s_name,
#    #    #    #   count(*) as numwait
#    #    #    # from
#    #    #    #   supplier,
#    #    #    #   lineitem l1,
#    #    #    #   orders,
#    #    #    #   nation
#    #    #    # where
#    #    #    #   s_suppkey = l1.l_suppkey
#    #    #    #   and o_orderkey = l1.l_orderkey
#    #    #    #   and o_orderstatus = 'F'
#    #    #    #   and l1.l_receiptdate > l1.l_commitdate
#    #    #    #   and exists (
#    #    #    #     select # ignore this subquery
#    #    #    #       *
#    #    #    #     from
#    #    #    #       lineitem l2
#    #    #    #     where
#    #    #    #       l2.l_orderkey = l1.l_orderkey
#    #    #    #       and l2.l_suppkey <> l1.l_suppkey
#    #    #    #   )
#    #    #    #   and not exists (
#    #    #    #     select
#    #    #    #       *
#    #    #    #     from
#    #    #    #       lineitem l3
#    #    #    #     where
#    #    #    #       l3.l_orderkey = l1.l_orderkey
#    #    #    #       and l3.l_suppkey <> l1.l_suppkey
#    #    #    #       and l3.l_receiptdate > l3.l_commitdate
#    #    #    #   )
#    #    #    #   and s_nationkey = n_nationkey
#    #    #    #   and n_name = '[NATION]'
#    #    #    # group by
#    #    #    #   s_name
#    #    #    # order by
#    #    #    #   numwait desc,
#    #    #    #   s_name;
#    #    #         Q 'SELECT lineitem.* FROM lineitem ' \
#    #    #           'WHERE lineitem.dummy = ? AND lineitem.l_receiptdate > ? AND lineitem.l_commitdate < ? -- Q21_inner'
#    #    #         Q 'SELECT to_supplier.s_name, count(lineitem.l_orderkey) ' \
#    #    #           'FROM lineitem.l_orderkey.o_custkey.c_nationkey ' \
#    #    #           'WHERE l_orderkey.o_orderstatus = ? AND lineitem.l_receiptdate > ? AND lineitem.l_commitdate < ? ' \
#    #    #             'AND lineitem.l_orderkey = ? AND to_nation.n_name = ? ' \
#    #    #           'ORDER BY to_supplier.s_name ' \
#    #    #           'GROUP BY to_supplier.s_name -- Q21_outer'
#    #    #
#    #    #    # == Q22 ==
#    #    #    # select
#    #    #    #     cntrycode,
#    #    #    #     count(*) as numcust,
#    #    #    #     sum(c_acctbal) as totacctbal
#    #    #    # from (
#    #    #    #     select # -- Q22
#    #    #    #         substring(c_phone from 1 for 2) as cntrycode,
#    #    #    #         c_acctbal
#    #    #    #     from
#    #    #    #         customer
#    #    #    #     where
#    #    #    #         substring(c_phone from 1 for 2) in
#    #    #    #         ('[I1]','[I2]’,'[I3]','[I4]','[I5]','[I6]','[I7]')
#    #    #    #         and c_acctbal > (
#    #    #    #             select  # -- Q22_inner_inner1
#    #    #    #               avg(c_acctbal)
#    #    #    #             from
#    #    #    #               customer
#    #    #    #             where
#    #    #    #               c_acctbal > 0.00
#    #    #    #               and substring (c_phone from 1 for 2) in
#    #    #    #               ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
#    #    #    #         )
#    #    #    #         and not exists (
#    #    #    #             select # this query was converted to customer.c_custkey = ?
#    #    #    #               *
#    #    #    #             from
#    #    #    #               orders
#    #    #    #             where
#    #    #    #               o_custkey = c_custkey
#    #    #    #         )
#    #    #    #     ) as custsale
#    #    #    # group by
#    #    #    # cntrycode
#    #    #    # order by
#    #    #    # cntrycode;
#    #    #         Q 'SELECT avg(customer.c_acctbal) FROM customer ' \
#    #    #           'WHERE customer.c_acctbal > ? AND customer.c_phone = ? -- Q22_inner_inner'
#    #    #         Q 'SELECT customer.c_phone, sum(customer.c_acctbal), count(customer.c_custkey) ' \
#    #    #           'FROM customer ' \
#    #    #           'WHERE customer.c_phone = ? AND customer.c_acctbal > ? AND customer.c_custkey = ? ' \
#    #    #           'ORDER BY customer.c_phone ' \
#    #    #           'GROUP BY customer.c_phone -- Q22'
  #  end
end
