module NoSE
  describe PrunedIndexEnumerator do
    include_context 'entities'
    include_context 'dummy cost model'
    subject(:pruned_enum) { PrunedIndexEnumerator.new workload, cost_model,
                                                      1, 100, 1 }

    it 'produces a simple index for a filter' do
      query = Statement.parse 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      indexes = pruned_enum.indexes_for_queries [query], []
      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [user['Username']],
                  QueryGraph::Graph.from_path([user.id_field])
      expect(indexes.size).to be 1
    end

    it 'produces a simple index for a foreign key join' do
      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = pruned_enum.indexes_for_queries [query], []
      expect(indexes).to include \
        Index.new [user['City']], [user['UserId'], tweet['TweetId']],
                  [tweet['Body']],
                  QueryGraph::Graph.from_path([user.id_field,
                                               user['Tweets']])
      expect(indexes.size).to be 1
    end

    it 'produces a simple index for a filter within a workload' do
      query = Statement.parse 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = pruned_enum.indexes_for_workload
      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [user['Username']],
                  QueryGraph::Graph.from_path([user.id_field])
      expect(indexes.size).to be 1
    end

    it 'does not produce empty indexes' do
      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = pruned_enum.indexes_for_workload
      expect(indexes).to all(satisfy do |index|
        !index.order_fields.empty? || !index.extra.empty?
      end)
      expect(indexes.size).to be 1
    end

    it 'includes no indexes for updates if nothing is updated' do
      # Use a fresh workload for this test
      model = workload.model
      workload = Workload.new model
      pruned_enum = PrunedIndexEnumerator.new workload, cost_model, 1,
                                              100, 1
      update = Statement.parse 'UPDATE User SET Username = ? ' \
                               'WHERE User.City = ?', model
      workload.add_statement update
      indexes = pruned_enum.indexes_for_workload
      expect(indexes).to be_empty
    end

    it 'includes indexes enumerated from queries generated from updates' do
      # Use a fresh workload for this test
      model = workload.model
      workload = Workload.new model
      pruned_enum = PrunedIndexEnumerator.new workload, cost_model, 1,
                                              100, 1

      update = Statement.parse 'UPDATE User SET Username = ? ' \
                               'WHERE User.City = ?', model
      workload.add_statement update

      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.Username = ?', workload.model
      workload.add_statement query

      indexes = pruned_enum.indexes_for_workload
      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [],
                  QueryGraph::Graph.from_path([user.id_field])

      expect(indexes.to_a).to include \
        Index.new [user['UserId']], [tweet['TweetId']],
                  [tweet['Body']],
                  QueryGraph::Graph.from_path([user.id_field,
                                               user['Tweets']])
      expect(indexes.size).to be 12
    end

    it 'enumerates only one index for each query if that are not overlapping' do
      tpch_workload = Workload.new do |_|
        Model 'tpch'
        DefaultMix :default
        Group 'Group1', default: 1 do
          Q 'SELECT to_supplier.s_acctbal '\
            'FROM part.from_partsupp.to_supplier ' \
            'WHERE part.p_size = ?'

          Q 'SELECT lineitem.l_orderkey '\
            'FROM lineitem.to_orders.to_customer '\
            'WHERE to_customer.c_mktsegment = ?'\
        end
      end
      indexes = PrunedIndexEnumerator.new(tpch_workload, cost_model,
                                          1, 2, 1)
                    .indexes_for_workload.to_a
      expect(indexes.size).to be 2
    end

    it 'enumerates indexes for complicated and overlapping queries and insert' do
      tpch_workload = Workload.new do |_|
        Model 'tpch'
        DefaultMix :default
        Group 'Group1', default: 1 do
          Q 'SELECT to_supplier.s_acctbal, to_supplier.s_name, to_nation.n_name, part.p_partkey, part.p_mfgr, '\
                'to_supplier.s_address, to_supplier.s_phone, to_supplier.s_comment ' \
                'FROM part.from_partsupp.to_supplier.to_nation.to_region ' \
                'WHERE part.p_size = ? AND part.p_type = ? AND from_partsupp.ps_supplycost = ?'

          Q 'SELECT lineitem.l_orderkey, lineitem.l_extendedprice, lineitem.l_discount, to_orders.o_orderdate, to_orders.o_shippriority '\
              'FROM lineitem.to_orders.to_customer '\
              'WHERE to_customer.c_mktsegment = ? AND to_orders.o_orderdate < ? AND lineitem.l_shipdate > ?'

          Q 'INSERT INTO nation SET n_nationkey=?, n_name=?, n_comment=? AND CONNECT TO to_region(?) -- 5'
        end
      end
      indexes = PrunedIndexEnumerator.new(tpch_workload, cost_model,
                                          1, 2, 1)
                    .indexes_for_workload.to_a
      expect(indexes.size).to be 260
    end

    it 'prunes indexes based on its used times among queries' do
      query1 = Statement.parse 'SELECT User.* FROM User ' \
                                                     'WHERE User.Username = ?', workload.model
      query2 = Statement.parse 'SELECT User.* FROM User ' \
                                                     'WHERE User.Username = ? AND User.City = ?', workload.model
      query3 = Statement.parse 'SELECT User.* FROM User ' \
                                                     'WHERE User.Username = ? AND User.City = ? AND User.Country = ?', workload.model
      workload.add_statement query1
      workload.add_statement query2
      workload.add_statement query3
      queries = [query1, query2, query3]

      indexes = PrunedIndexEnumerator.new(workload, cost_model, 1,
                                          100, 1)
                    .indexes_for_workload

      shared_by_2_queries = PrunedIndexEnumerator.new(workload, cost_model,
                                                 1, 100, 2)
                           .pruning_tree_by_is_shared(queries, indexes)

      expect(shared_by_2_queries.size).to be 51

      shared_by_3_queries = PrunedIndexEnumerator.new(workload, cost_model,
                                                  1, 100, 3)
                            .pruning_tree_by_is_shared(queries, indexes)
      # materialize views for each query are remained
      expect(shared_by_3_queries.size).to be 3
    end
  end
end
