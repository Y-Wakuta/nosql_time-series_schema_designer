module NoSE
  describe GraphBasedIndexEnumerator do
    include_context 'entities'
    include_context 'dummy cost model'
    subject(:graph_based_enum) { GraphBasedIndexEnumerator.new workload, cost_model, 2,10000 }

    it 'produces a simple index for a filter' do
      query = Statement.parse 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      indexes = graph_based_enum.indexes_for_queries [query], []
      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [user['Username']],
                  QueryGraph::Graph.from_path([user.id_field])
    end

    it 'produces a simple index for a foreign key join' do
      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = graph_based_enum.indexes_for_queries [query], []
      expect(indexes).to include \
        Index.new [user['City']], [user['UserId'], tweet['TweetId']],
                  [tweet['Body']],
                  QueryGraph::Graph.from_path([user.id_field,
                                               user['Tweets']])
    end

    it 'produces a simple index for a filter within a workload' do
      query = Statement.parse 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = graph_based_enum.indexes_for_workload
      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [user['Username']],
                  QueryGraph::Graph.from_path([user.id_field])
    end

    it 'does not produce empty indexes' do
      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = graph_based_enum.indexes_for_workload
      expect(indexes).to all(satisfy do |index|
        !index.order_fields.empty? || !index.extra.empty?
      end)
    end

    it 'includes no indexes for updates if nothing is updated' do
      # Use a fresh workload for this test
      model = workload.model
      workload = Workload.new model
      graph_based_enum = PrunedIndexEnumerator.new workload, cost_model, 1,
                                              100, 1
      update = Statement.parse 'UPDATE User SET Username = ? ' \
                               'WHERE User.City = ?', model
      workload.add_statement update
      indexes = graph_based_enum.indexes_for_workload
      expect(indexes).to be_empty
    end

    it 'includes indexes enumerated from queries generated from updates' do
      # Use a fresh workload for this test
      model = workload.model
      workload = Workload.new model
      graph_based_enum = PrunedIndexEnumerator.new workload, cost_model, 1,
                                              100, 1

      update = Statement.parse 'UPDATE User SET Username = ? ' \
                               'WHERE User.City = ?', model
      workload.add_statement update

      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.Username = ?', workload.model
      workload.add_statement query

      indexes = graph_based_enum.indexes_for_workload
      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [],
                  QueryGraph::Graph.from_path([user.id_field])

      expect(indexes.to_a).to include \
        Index.new [user['UserId']], [tweet['TweetId']],
                  [tweet['Body']],
                  QueryGraph::Graph.from_path([user.id_field,
                                               user['Tweets']])
    end

    it 'produces indexes that include aggregation processes' do
      query = Statement.parse 'SELECT count(Tweet.Body), count(Tweet.TweetId), sum(User.UserId), avg(Tweet.Retweets) FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = graph_based_enum.indexes_for_queries [query], []
      expect(indexes.map(&:count_fields)).to include [tweet['Body'], tweet['TweetId']]
      expect(indexes.map(&:sum_fields)).to include [user['UserId']]
      expect(indexes.map(&:avg_fields)).to include [tweet['Retweets']]
    end

    it 'makes sure that all aggregation fields are included in index fields' do
      query = Statement.parse 'SELECT count(Tweet.Body), count(Tweet.TweetId), sum(User.UserId), avg(Tweet.Retweets) FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = graph_based_enum.indexes_for_queries [query], []
      indexes.each do |index|
        expect(index.all_fields).to be >= (index.count_fields + index.sum_fields + index.avg_fields).to_set
      end
    end
  end

  describe PrunedIndexEnumerator do
      include_context 'entities'
      include_context 'dummy cost model'

    it 'prunes indexes based on its used times among queries' do
      query1 = Statement.parse 'SELECT User.* FROM User ' \
                                                     'WHERE User.Username = ?', workload.model
      query2 = Statement.parse 'SELECT User.* FROM User ' \
                                                     'WHERE User.Username = ? AND User.City = ?', workload.model
      query3 = Statement.parse 'SELECT User.* FROM User ' \
                                                     'WHERE User.Username = ? AND User.City = ? AND User.Country = ?', workload.model
      update = Statement.parse 'INSERT INTO User SET UserId = ?, Username = ?', workload.model
      workload.add_statement query1
      workload.add_statement query2
      workload.add_statement query3
      workload.add_statement update
      queries = [query1, query2, query3]

      indexes = PrunedIndexEnumerator.new(workload, cost_model, 1,
                                          100, 1)
                    .indexes_for_workload

      shared_by_2_queries = PrunedIndexEnumerator.new(workload, cost_model,
                                                 1, 100, 2)
                           .pruning_tree_by_is_shared(queries, indexes)

      expect(shared_by_2_queries.size).to be 57

      shared_by_3_queries = PrunedIndexEnumerator.new(workload, cost_model,
                                                  1, 100, 3)
                            .pruning_tree_by_is_shared(queries, indexes)
      # materialize views for each query are remained
      expect(shared_by_3_queries.size).to be 52
    end
  end
end
