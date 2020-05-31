module NoSE
  describe IndexEnumerator do
    include_context 'entities'

    subject(:enum) { IndexEnumerator.new workload }

    it 'produces a simple index for a filter' do
      query = Statement.parse 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      indexes = enum.indexes_for_query query

      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [user['Username']],
                  QueryGraph::Graph.from_path([user.id_field])
      expect(indexes.size).to be 9
    end

    it 'produces a simple index for a foreign key join' do
      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = enum.indexes_for_query query

      expect(indexes).to include \
        Index.new [user['City']], [user['UserId'], tweet['TweetId']],
                  [tweet['Body']],
                  QueryGraph::Graph.from_path([user.id_field,
                                               user['Tweets']])
      expect(indexes.size).to be 25
    end

    it 'produces an index for intermediate query steps' do
      query = Statement.parse 'SELECT Link.URL FROM Link.Tweets.User ' \
                              'WHERE User.Username = ?', workload.model
      indexes = enum.indexes_for_query query
      expect(indexes).to include \
        Index.new [user['UserId']], [tweet['TweetId']], [],
                  QueryGraph::Graph.from_path([tweet.id_field,
                                               tweet['User']])
      expect(indexes.size).to be 87
    end

    it 'produces a simple index for a filter within a workload' do
      query = Statement.parse 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = enum.indexes_for_workload

      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [user['Username']],
                  QueryGraph::Graph.from_path([user.id_field])
      expect(indexes.size).to be 8
    end

    it 'does not produce empty indexes' do
      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = enum.indexes_for_workload
      expect(indexes).to all(satisfy do |index|
        !index.order_fields.empty? || !index.extra.empty?
      end)
      expect(indexes.size).to be 24
    end

    it 'includes no indexes for updates if nothing is updated' do
      # Use a fresh workload for this test
      model = workload.model
      workload = Workload.new model
      enum = IndexEnumerator.new workload
      update = Statement.parse 'UPDATE User SET Username = ? ' \
                               'WHERE User.City = ?', model
      workload.add_statement update
      indexes = enum.indexes_for_workload

      expect(indexes).to be_empty
    end

    it 'includes indexes enumerated from queries generated from updates' do
      # Use a fresh workload for this test
      model = workload.model
      workload = Workload.new model
      enum = IndexEnumerator.new workload

      update = Statement.parse 'UPDATE User SET Username = ? ' \
                               'WHERE User.City = ?', model
      workload.add_statement update

      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.Username = ?', workload.model
      workload.add_statement query

      indexes = enum.indexes_for_workload

      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [],
                  QueryGraph::Graph.from_path([user.id_field])

      expect(indexes.to_a).to include \
        Index.new [user['UserId']], [tweet['TweetId']],
                  [tweet['Body']],
                  QueryGraph::Graph.from_path([user.id_field,
                                               user['Tweets']])
      expect(indexes.size).to be 28
    end

    it 'produces indexes that include aggregation processes' do
      query = Statement.parse 'SELECT count(Tweet.Body), count(Tweet.TweetId), sum(User.UserId), avg(Tweet.Retweets) FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = enum.indexes_for_query query
      expect(indexes.map(&:count_fields)).to include [tweet['Body'], tweet['TweetId']]
      expect(indexes.map(&:sum_fields)).to include [user['UserId']]
      expect(indexes.map(&:avg_fields)).to include [tweet['Retweets']]
      expect(indexes.size).to be 34
    end

    it 'makes sure that all aggregation fields are included in index fields' do
      query = Statement.parse 'SELECT count(Tweet.Body), count(Tweet.TweetId), sum(User.UserId), avg(Tweet.Retweets) FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = enum.indexes_for_query query
      indexes.each do |index|
        expect(index.all_fields).to be >= (index.count_fields + index.sum_fields + index.avg_fields)
      end
      expect(indexes.size).to be 34
    end

    it 'only enumerates indexes with hash_fields that satisfy GROUP BY clause' do
      query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model
      indexes = enum.indexes_for_query query
      expect(indexes.any?{|i| i.hash_fields >= Set.new([tweet['Retweets']])}).to be(true)
      expect(indexes.size).to be 51
    end

    describe PrunedIndexEnumerator do
      it 'enumerates indexes for complicated queries and insert' do
        tpch_workload = Workload.new do |_|
          Model 'tpch'
          DefaultMix :default
          Group 'Group1', default: 1 do
            Q 'SELECT to_supplier.s_acctbal, to_supplier.s_name, to_nation.n_name, part.p_partkey, part.p_mfgr, '\
              'to_supplier.s_address, to_supplier.s_phone, to_supplier.s_comment ' \
              'FROM part.from_partsupp.to_supplier.to_nation.to_region ' \
              'WHERE part.p_size = ? AND part.p_type = ? AND to_region.r_name = ? AND from_partsupp.ps_supplycost = ? '\
              'ORDER BY to_supplier.s_acctbal, to_nation.n_name, to_supplier.s_name -- Q2_outer'

            Q 'SELECT lineitem.l_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), to_orders.o_orderdate, to_orders.o_shippriority '\
              'FROM lineitem.to_orders.to_customer '\
              'WHERE to_customer.c_mktsegment = ? AND to_orders.o_orderdate < ? AND lineitem.l_shipdate > ? '\
              'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, to_orders.o_orderdate ' \
              'GROUP BY lineitem.l_orderkey, to_orders.o_orderdate, to_orders.o_shippriority -- Q3'

            Q 'INSERT INTO lineitem SET l_orderkey=?, l_linenumber=?, l_quantity=?, l_extendedprice=?, l_discount=?, ' \
                  'l_tax = ?, l_returnflag=?, l_linestatus=?, l_shipdate=?, l_commitdate=?, l_receiptdate=?, ' \
                  'l_shipmode=?, l_comment=? AND CONNECT TO to_partsupp(?), to_orders(?) -- 1'
          end
        end
        indexes = PrunedIndexEnumerator.new(tpch_workload).indexes_for_workload.to_a
        expect(indexes.size).to be > 1
      end
    end
  end
end
