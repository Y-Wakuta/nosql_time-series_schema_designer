# frozen_string_literal: true

NoSE::Workload.new do
  Model 'tpch'

  # Define queries and their relative weights, weights taken from below
  DefaultMix :default

  # 1対多の多の側を更新する時は1の entity に CONNECT する
#  Group 'Upseart', default: 1000 do
#    Q 'INSERT INTO lineitem SET l_orderkey=?, l_linenumber=?, l_quantity=?, l_extendedprice=?, l_discount=?, ' \
#                  'l_tax = ?, l_returnflag=?, l_linestatus=?, l_shipdate=?, l_commitdate=?, l_receiptdate=?, ' \
#                  'l_shipmode=?, l_comment=? AND CONNECT TO to_partsupp(?), to_orders(?) -- 1'
#    Q 'INSERT INTO part SET p_partkey=?, p_name=?, p_mfgr=?, p_brand=?, p_type=?, p_size=?, p_container=?, ' \
#                  'p_retailprice=?, p_comment=? -- 2'
#    Q 'INSERT INTO partsupp SET ps_partkey=?, ps_suppkey=?, ps_availqty=?, ps_supplycost=?, ps_comment=? ' \
#                  'AND CONNECT TO to_part(?), to_supplier(?) -- 3'
#    Q 'INSERT INTO orders SET o_orderkey=?, o_orderstatus=?, o_totalprice=?, o_orderdate=?, o_orderpriority=?, '\
#                  'o_clerk=?, o_shippriority=?, o_comment=? AND CONNECT TO to_customer(?) -- 4'
#    Q 'INSERT INTO nation SET n_nationkey=?, n_name=?, n_comment=? AND CONNECT TO to_region(?) -- 5'
#    Q 'INSERT INTO region SET r_regionkey=?, r_name=?, r_comment=? -- 6'
#    Q 'INSERT INTO supplier SET s_suppkey=?, s_name=?, s_address=?, s_phone=?, s_acctbal=?, s_comment=? '\
#                  'AND CONNECT TO from_customer(?), to_nation(?) -- 7'
#  end

  Group 'Group1', default: 1 do
    # === Q1 ===
    #   select
    #     l_returnflag,
    #     l_linestatus,
    #     sum(l_quantity) as sum_qty,
    #     sum(l_extendedprice) as sum_base_price,
    #     sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    #     sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    #     avg(l_quantity) as avg_qty,
    #     avg(l_extendedprice) as avg_price,
    #     avg(l_discount) as avg_disc,
    #     count(*) as count_order
    #   from
    #     lineitem
    #   where
    #     l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
    #   group by
    #     l_returnflag,
    #     l_linestatus
    #   order by
    #     l_returnflag,
    #     l_linestatus;
    #

         Q 'SELECT lineitem.l_returnflag, lineitem.l_linestatus, sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), '\
             'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), sum(lineitem.l_extendedprice), '\
             'sum(lineitem.l_discount), sum(lineitem.l_tax), avg(lineitem.l_quantity), '\
             'avg(lineitem.l_extendedprice), avg(lineitem.l_discount), '\
             'count(lineitem.l_orderkey), count(lineitem.l_linenumber), count(lineitem.l_quantity), '\
             'count(lineitem.l_extendedprice), count(lineitem.l_discount), count(lineitem.l_tax), ' \
             'count(lineitem.l_returnflag), count(lineitem.l_linestatus), count(lineitem.l_shipdate), ' \
             'count(lineitem.l_commitdate), count(lineitem.l_shipmode), count(lineitem.l_comment) ' \
           'FROM lineitem '\
           'WHERE lineitem.l_orderkey = ? AND lineitem.l_linenumber = ? AND lineitem.l_shipdate <= ? ' \
           'GROUP BY lineitem.l_returnflag, lineitem.l_linestatus -- Q1'

    # === Q2 ===
    #    select
    #      s_acctbal,
    #      s_name,
    #      n_name,
    #      p_partkey,
    #      p_mfgr,
    #      s_address,
    #      s_phone,
    #      s_comment
    #    from
    #      part,
    #      supplier,
    #      partsupp,
    #      nation,
    #      region
    #    where
    #      p_partkey = ps_partkey
    #      and s_suppkey = ps_suppkey
    #      and p_size = [SIZE]
    #      and p_type like '%[TYPE]'
    #      and s_nationkey = n_nationkey
    #      and n_regionkey = r_regionkey
    #      and r_name = '[REGION]'
    #      and ps_supplycost = (
    #        select min(ps_supplycost)
    #        from
    #          partsupp, supplier,
    #          nation, region
    #        where
    #          p_partkey = ps_partkey
    #          and s_suppkey = ps_suppkey
    #          and s_nationkey = n_nationkey
    #          and n_regionkey = r_regionkey
    #          and r_name = '[REGION]'
    #        )
    #    order by
    #      s_acctbal desc,
    #      n_name,
    #      s_name,
    #      p_partkey;

         Q 'SELECT to_supplier.s_acctbal, to_supplier.s_name, to_nation.n_name, part.p_partkey, part.p_mfgr, '\
         'to_supplier.s_address, to_supplier.s_phone, to_supplier.s_comment ' \
       'FROM part.from_partsupp.to_supplier.to_nation.to_region ' \
       'WHERE part.p_size = ? AND part.p_type = ? AND to_region.r_name = ? AND from_partsupp.ps_supplycost = ? '\
       'ORDER BY to_supplier.s_acctbal, to_nation.n_name, to_supplier.s_name -- Q2_outer'

    ##  # TODO: this query originaly SELECTs min(partsupp.ps_supplycost). I need to add 'min' feature like 'max'.
    Q 'SELECT max(partsupp.ps_supplycost) FROM partsupp.to_supplier.to_nation.to_region '\
      'WHERE to_region.r_name = ? -- Q2_inner'

    # === Q3 ===
    #   select
    #     l_orderkey,
    #     sum(l_extendedprice*(1-l_discount)) as revenue,
    #     o_orderdate,
    #     o_shippriority
    #   from
    #     customer,
    #     orders,
    #     lineitem
    #   where
    #     c_mktsegment = '[SEGMENT]'
    #     and c_custkey = o_custkey
    #     and l_orderkey = o_orderkey
    #     and o_orderdate < date '[DATE]'
    #     and l_shipdate > date '[DATE]'
    #   group by
    #     l_orderkey,
    #     o_orderdate,
    #     o_shippriority
    #   order by
    #    revenue desc,
    #    o_orderdate;

            Q 'SELECT lineitem.l_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), to_orders.o_orderdate, to_orders.o_shippriority '\
       'FROM lineitem.to_orders.to_customer '\
       'WHERE to_customer.c_mktsegment = ? AND to_orders.o_orderdate < ? AND lineitem.l_shipdate > ? '\
       'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, to_orders.o_orderdate ' \
       'GROUP BY lineitem.l_orderkey, to_orders.o_orderdate, to_orders.o_shippriority -- Q3'


    # === Q4 ===
    #   select
    #     o_orderpriority,
    #     count(*) as order_count
    #   from
    #     orders
    #   where
    #     o_orderdate >= date '[DATE]'
    #     and o_orderdate < date '[DATE]' + interval '3' month
    #     and exists (
    #        select
    #        *
    #        from
    #        lineitem
    #        where
    #        l_orderkey = o_orderkey
    #        and l_commitdate < l_receiptdate
    #     )
    #   group by
    #     o_orderpriority
    #   order by
    #     o_orderpriority;

         # this query only has one eq predicate and the predicates uses primary key field. Therefore, this query possibly does not use join plans
     Q 'SELECT to_orders.o_orderpriority, count(to_orders.o_orderkey), count(to_orders.o_orderstatus), count(to_orders.o_totalprice), ' \
      'count(to_orders.o_orderdate), count(to_orders.o_orderpriority), count(to_orders.o_clerk), count(to_orders.o_shippriority), count(to_orders.o_comment) ' \
      'FROM lineitem.to_orders '\
      'WHERE to_orders.o_orderkey = ? AND to_orders.o_orderdate >= ? AND to_orders.o_orderdate < ? AND lineitem.l_commitdate < ? AND lineitem.l_receiptdate > ? ' \
      'ORDER BY to_orders.o_orderpriority ' \
      'GROUP BY to_orders.o_orderpriority -- Q4'

    # === Q5 ===
    #   select
    #     n_name,
    #     sum(l_extendedprice * (1 - l_discount)) as revenue
    #   from
    #     customer,
    #     orders,
    #     lineitem,
    #     supplier,
    #     nation,
    #     region
    #   where
    #     c_custkey = o_custkey
    #     and l_orderkey = o_orderkey
    #     and l_suppkey = s_suppkey
    #     and c_nationkey = s_nationkey
    #     and s_nationkey = n_nationkey
    #     and n_regionkey = r_regionkey
    #     and r_name = '[REGION]'
    #     and o_orderdate >= date '[DATE]'
    #     and o_orderdate < date '[DATE]' + interval '1' year
    #   group by
    #     n_name
    #   order by
    #     revenue desc;
    #   'WHERE to_region.r_name = ? AND to_orders.o_orderdate >= ? AND to_orders.o_orderdate < ? ' \


         # このクエリも Q4 と同様に１つしか eq predicate を持たないが，その条件は primary key 属性を使用している訳では無いし SELECT 句の中も region を参照していないのでこちらは多数のジョインプランが出せるはず
    Q 'SELECT to_nation.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
  'FROM lineitem.to_orders.to_customer.to_nation.to_region ' \
  'WHERE to_region.r_name = ? AND to_orders.o_orderdate >= ? AND to_orders.o_orderdate < ? ' \
  'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
  'GROUP BY to_nation.n_name -- Q5'


    # === Q6 ===
    #   select
    #     sum(l_extendedprice*l_discount) as revenue
    #   from
    #     lineitem
    #   where
    #     l_shipdate >= date '[DATE]'
    #     and l_shipdate < date '[DATE]' + interval '1' year
    #     and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
    #     and l_quantity < [QUANTITY];

    Q 'SELECT sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem ' \
      'WHERE lineitem.l_orderkey = ? AND lineitem.l_shipdate >= ? AND lineitem.l_shipdate < ? ' \
          'AND lineitem.l_discount > ? AND lineitem.l_discount < ? ' \
          'AND lineitem.l_quantity < ? -- Q6'

    # === Q7 ===
    #   select
    #     supp_nation,
    #     cust_nation,
    #     l_year, sum(volume) as revenue
    #   from (
    #         select
    #           n1.n_name as supp_nation,
    #           n2.n_name as cust_nation,
    #           extract(year from l_shipdate) as l_year,
    #           l_extendedprice * (1 - l_discount) as volume
    #         from
    #           supplier,
    #           lineitem,
    #           orders,
    #           customer,
    #           nation n1,
    #           nation n2
    #         where
    #           s_suppkey = l_suppkey
    #           and o_orderkey = l_orderkey
    #           and c_custkey = o_custkey
    #           and s_nationkey = n1.n_nationkey
    #           and c_nationkey = n2.n_nationkey
    #           and (
    #           (n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]')
    #           or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]')
    #           )
    #           and l_shipdate between date '1995-01-01' and date '1996-12-31'
    #         ) as shipping
    #   group by
    #     supp_nation,
    #     cust_nation,
    #     l_year
    #   order by
    #     supp_nation,
    #     cust_nation,
    #     l_year;

    Q 'SELECT to_nation.n_name, lineitem.l_shipdate, '\
              'sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
      'FROM lineitem.to_orders.to_customer.to_nation '\
      'WHERE lineitem.l_orderkey = ? AND lineitem.l_shipdate < ? AND lineitem.l_shipdate > ? ' \
      'ORDER BY to_nation.n_name, lineitem.l_shipdate ' \
      'GROUP BY to_nation.n_name, lineitem.l_shipdate -- Q7'

    # === Q8 ===
    #   select
    #     o_year,
    #     sum(case
    #     when nation = '[NATION]'
    #     then volume
    #     else 0
    #     end) / sum(volume) as mkt_share
    #   from (
    #        select
    #          extract(year from o_orderdate) as o_year,
    #          l_extendedprice * (1-l_discount) as volume,
    #          n2.n_name as nation
    #        from
    #          part,
    #          supplier,
    #          lineitem,
    #          orders,
    #          customer,
    #          nation n1,
    #          nation n2,
    #          region
    #        where
    #          p_partkey = l_partkey
    #          and s_suppkey = l_suppkey
    #          and l_orderkey = o_orderkey
    #          and o_custkey = c_custkey
    #          and c_nationkey = n1.n_nationkey
    #          and n1.n_regionkey = r_regionkey
    #          and r_name = '[REGION]'
    #          and s_nationkey = n2.n_nationkey
    #          and o_orderdate between date '1995-01-01' and date '1996-12-31'
    #          and p_type = '[TYPE]'
    #     ) as all_nations
    #   group by
    #     o_year
    #     order by
    #     o_year;

    Q 'SELECT to_orders.o_orderdate, from_lineitem.l_extendedprice, from_lineitem.l_discount, to_nation.n_name '\
      'FROM part.from_partsupp.from_lineitem.to_orders.to_customer.to_nation.to_region ' \
      'WHERE to_region.r_name = ? AND to_orders.o_orderdate < ? AND to_orders.o_orderdate > ? AND part.p_type = ? ' \
      'ORDER BY to_orders.o_orderdate ' \
      'GROUP BY to_orders.o_orderdate -- Q8'

    # === Q9 ===
    #   select
    #     nation,
    #     o_year,
    #     sum(amount) as sum_profit
    #   from (
    #        select
    #          n_name as nation,
    #          extract(year from o_orderdate) as o_year,
    #          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    #        from
    #          part,
    #          supplier,
    #          lineitem,
    #          partsupp,
    #          orders,
    #          nation
    #        where
    #          s_suppkey = l_suppkey
    #          and ps_suppkey = l_suppkey
    #          and ps_partkey = l_partkey
    #          and p_partkey = l_partkey
    #          and o_orderkey = l_orderkey
    #          and s_nationkey = n_nationkey
    #          and p_name like '%[COLOR]%'
    #      ) as profit
    #   group by
    #     nation,
    #     o_year
    #   order by
    #     nation,
    #     o_year desc;

    Q 'SELECT to_nation.n_name, to_orders.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount), '  \
          'sum(from_partsupp.ps_supplycost), sum(from_lineitem.l_quantity) ' \
      'FROM part.from_partsupp.from_lineitem.to_orders.to_customer.to_nation ' \
      'WHERE part.p_name = ? AND from_lineitem.l_orderkey = ? ' \
      'ORDER BY to_nation.n_name, to_orders.o_orderdate ' \
      'GROUP BY to_nation.n_name, to_orders.o_orderdate -- Q9'

    # === Q10 ===
    #   select
    #      c_custkey,
    #      c_name,
    #      sum(l_extendedprice * (1 - l_discount)) as revenue,
    #      c_acctbal,
    #      n_name,
    #      c_address,
    #      c_phone,
    #      c_comment
    #   from
    #      customer,
    #      orders,
    #      lineitem,
    #      nation
    #   where
    #      c_custkey = o_custkey
    #      and l_orderkey = o_orderkey
    #      and o_orderdate >= date '[DATE]'
    #      and o_orderdate < date '[DATE]' + interval '3' month
    #      and l_returnflag = 'R'
    #      and c_nationkey = n_nationkey
    #   group by
    #      c_custkey,
    #      c_name,
    #      c_acctbal,
    #      c_phone,
    #      n_name,
    #      c_address,
    #      c_comment
    #   order by
    #      revenue desc;

    Q 'SELECT to_customer.c_custkey, to_customer.c_name, '\
          'sum(lineitem.l_extendedprice), sum(lineitem.l_discount), '\
          'to_customer.c_acctbal, to_nation.n_name, '\
          'to_customer.c_address, to_customer.c_phone, to_customer.c_comment '\
       'FROM lineitem.to_orders.to_customer.to_nation '\
       'WHERE to_orders.o_orderdate >= ? AND to_orders.o_orderdate < ? AND lineitem.l_returnflag = ? '\
       'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
       'GROUP BY to_customer.c_custkey, to_customer.c_name, to_customer.c_acctbal, '\
          'to_customer.c_phone, to_nation.n_name, to_customer.c_address, to_customer.c_comment -- Q10'

    # === Q11 ===
    #   select
    #      ps_partkey,
    #      sum(ps_supplycost * ps_availqty) as value
    #   from
    #      partsupp,
    #      supplier,
    #      nation
    #   where
    #      ps_suppkey = s_suppkey
    #      and s_nationkey = n_nationkey
    #      and n_name = '[NATION]'
    #   group by
    #      ps_partkey
    #   having
    #      sum(ps_supplycost * ps_availqty) > (
    #        select
    #          sum(ps_supplycost * ps_availqty) * [FRACTION]
    #        from
    #          partsupp,
    #          supplier,
    #          nation
    #        where
    #          ps_suppkey = s_suppkey
    #          and s_nationkey = n_nationkey
    #          and n_name = '[NATION]'
    #      )
    #   order by
    #      value desc;
    Q 'SELECT partsupp.ps_partkey, sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) ' \
      'FROM partsupp.to_supplier.to_nation '\
      'WHERE to_nation.n_name = ? AND partsupp.ps_supplycost = ? AND partsupp.ps_availqty = ? '\
      'ORDER BY partsupp.ps_supplycost, partsupp.ps_availqty ' \
      'GROUP BY partsupp.ps_partkey -- Q11_outer'
    Q 'SELECT sum(partsupp.ps_supplycost), sum(partsupp.ps_availqty) '\
      'FROM partsupp.to_supplier.to_nation '\
      'WHERE to_nation.n_name = ? -- Q11_inner'

    # === Q12 ===
    #   select
    #       l_shipmode,
    #       sum(case
    #          when o_orderpriority ='1-URGENT'
    #          or o_orderpriority ='2-HIGH'
    #          then 1
    #          else 0
    #       end) as high_line_count,
    #       sum(case
    #          when o_orderpriority <> '1-URGENT'
    #          and o_orderpriority <> '2-HIGH'
    #          then 1
    #          else 0
    #       end) as low_line_count
    #   from
    #      orders,
    #      lineitem
    #   where
    #      o_orderkey = l_orderkey
    #      and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
    #      and l_commitdate < l_receiptdate
    #      and l_shipdate < l_commitdate
    #      and l_receiptdate >= date '[DATE]'
    #      and l_receiptdate < date '[DATE]' + interval '1' year
    #   group by
    #      l_shipmode
    #   order by
    #      l_shipmode;
    Q 'SELECT lineitem.l_shipmode, sum(to_orders.o_orderpriority) '\
      'FROM lineitem.to_orders '\
      'WHERE lineitem.l_shipmode = ? AND lineitem.l_commitdate < ? ' \
          'AND lineitem.l_commitdate > ? AND lineitem.l_shipdate < ? ' \
          'AND lineitem.l_receiptdate > ? AND lineitem.l_receiptdate >= ? AND lineitem.l_receiptdate < ? ' \
      'ORDER BY lineitem.l_shipmode ' \
      'GROUP BY lineitem.l_shipmode -- Q12'

    # === Q13 ===
    #select
    #   c_count, count(*) as custdist
    #from (
    #    select
    #      c_custkey,
    #      count(o_orderkey)
    #    from
    #      customer left outer join orders on
    #      c_custkey = o_custkey
    #      and o_comment not like ‘%[WORD1]%[WORD2]%’
    #    group by
    #      c_custkey
    #  )as c_orders (c_custkey, c_count)
    #group by
    #   c_count
    #order by
    #   custdist desc,
    #   c_count desc;
    Q 'SELECT to_customer.c_custkey, count(orders.o_orderkey) ' \
      'FROM orders.to_customer ' \
      'WHERE orders.o_comment = ? ' \
      'GROUP BY to_customer.c_custkey, orders.o_orderkey -- Q13'

    # === Q14 ===
    #   select
    #      100.00 * sum(case
    #         when p_type like 'PROMO%'
    #         then l_extendedprice*(1-l_discount)
    #         else 0
    #      end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    #   from
    #      lineitem,
    #      part
    #   where
    #      l_partkey = p_partkey
    #      and l_shipdate >= date '[DATE]'
    #      and l_shipdate < date '[DATE]' + interval '1' month;
    Q 'SELECT sum(to_part.p_type), sum(lineitem.l_extendedprice), sum(lineitem.l_discount) '\
      'FROM lineitem.to_partsupp.to_part '\
      'WHERE lineitem.l_orderkey = ? AND lineitem.l_shipdate >= ? AND lineitem.l_shipdate < ? -- Q14'

    # === Q15 ===
    #   create view revenue[STREAM_ID] (supplier_no, total_revenue) as
    #       select
    #       l_suppkey,
    #       sum(l_extendedprice * (1 - l_discount))
    #       from
    #       lineitem
    #       where
    #       l_shipdate >= date '[DATE]'
    #       and l_shipdate < date '[DATE]' + interval '3' month
    #       group by
    #       l_suppkey;
    #
    #   select
    #      s_suppkey,
    #      s_name,
    #      s_address,
    #      s_phone,
    #      total_revenue
    #   from
    #      supplier,
    #      revenue[STREAM_ID]
    #   where
    #      s_suppkey = supplier_no
    #      and total_revenue = (
    #        select
    #        max(total_revenue)
    #        from
    #        revenue[STREAM_ID]
    #      )
    #   order by
    #      s_suppkey;
    Q 'SELECT to_partsupp.ps_suppkey, max(lineitem.l_extendedprice), max(lineitem.l_discount) ' \
      'FROM lineitem.to_partsupp '\
      'WHERE lineitem.l_orderkey = ? AND lineitem.l_shipdate >= ? AND lineitem.l_shipdate < ? ' \
      'GROUP BY to_partsupp.ps_suppkey -- Q15_inner'

      #Q 'select
    ##      s_suppkey,
    ##      s_name,
    ##      s_address,
    ##      s_phone,
    ##      total_revenue
    ##   from'
  end
end
