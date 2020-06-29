module NoSE
  describe PatternMiner do
    include_context 'entities'

    #it 'mines field pattern for simple query' do
    #  query1 = Statement.parse 'SELECT User.Username, User.UserId FROM User ' \
    #                                'WHERE User.City = ? GROUP BY User.UserId', workload.model
    #  query2 = Statement.parse 'SELECT User.Username FROM User ' \
    #                                'WHERE User.City = ?', workload.model
    #  query3 = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
    #                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model
    #  workload.add_statement query1, 1
    #  workload.add_statement query2, 1
    #  workload.add_statement query3, 10
    #  indexes = PrunedIndexEnumerator.new(workload).indexes_for_workload.to_a
    #  basic_size = indexes.size

    #  pattern_miner = PatternMiner.new
    #  pattern_miner.pattern_for_workload workload
    #  indexes = pattern_miner.validate_indexes indexes
    #  expect(basic_size).to be > indexes.size
    #end

    #it 'enumerates indexes for complicated queries and insert' do
    #  tpch_workload = Workload.new do |_|
    #    Model 'tpch'
    #    DefaultMix :default
    #    Group 'Group1', default: 10 do
    #      Q 'SELECT to_supplier.s_acctbal, to_supplier.s_name, to_nation.n_name, part.p_partkey, part.p_mfgr, '\
    #          'to_supplier.s_address, to_supplier.s_phone, to_supplier.s_comment ' \
    #          'FROM part.from_partsupp.to_supplier.to_nation.to_region ' \
    #          'WHERE part.p_size = ? AND part.p_type = ? AND to_region.r_name = ? AND from_partsupp.ps_supplycost = ? '\
    #          'ORDER BY to_supplier.s_acctbal, to_nation.n_name, to_supplier.s_name -- Q2_outer'
    #    end

    #    Group 'Group2', default: 1 do
    #      Q 'SELECT lineitem.l_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), to_orders.o_orderdate, to_orders.o_shippriority '\
    #          'FROM lineitem.to_orders.to_customer '\
    #          'WHERE to_customer.c_mktsegment = ? AND to_orders.o_orderdate < ? AND lineitem.l_shipdate > ? '\
    #          'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, to_orders.o_orderdate ' \
    #          'GROUP BY lineitem.l_orderkey, to_orders.o_orderdate, to_orders.o_shippriority -- Q3'
    #    end
    #  end
    #  indexes = PrunedIndexEnumerator.new(tpch_workload).indexes_for_workload.to_a
    #  basic_size = indexes.size

    #  pattern_miner = PatternMiner.new
    #  pattern_miner.pattern_for_workload tpch_workload
    #  tpch_workload.statement_weights.keys.each do |query|
    #    pattern_miner.validate_indexes indexes
    #  end

    #  expect(indexes.size).to be > 1
    #end

  end
end
