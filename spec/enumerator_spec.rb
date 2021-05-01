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
    end

    it 'produces an index for intermediate query steps' do
      query = Statement.parse 'SELECT Link.URL FROM Link.Tweets.User ' \
                              'WHERE User.Username = ?', workload.model
      indexes = enum.indexes_for_query query
      expect(indexes).to include \
        Index.new [user['UserId']], [tweet['TweetId']], [],
                  QueryGraph::Graph.from_path([tweet.id_field,
                                               tweet['User']])
    end

    it 'produces a simple index for a filter within a workload' do
      query = Statement.parse 'SELECT User.Username FROM User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = enum.indexes_for_workload
      expect(indexes.to_a).to include \
        Index.new [user['City']], [user['UserId']], [user['Username']],
                  QueryGraph::Graph.from_path([user.id_field])
    end

    it 'does not produce empty indexes' do
      query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      workload.add_statement query
      indexes = enum.indexes_for_workload
      expect(indexes).to all(satisfy do |index|
        !index.order_fields.empty? || !index.extra.empty?
      end)
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
    end

    it 'makes sure that all aggregation fields are included in index fields' do
      query = Statement.parse 'SELECT count(Tweet.Body), count(Tweet.TweetId), sum(User.UserId), avg(Tweet.Retweets) FROM Tweet.User ' \
                              'WHERE User.City = ?', workload.model
      indexes = enum.indexes_for_query query
      indexes.each do |index|
        expect(index.all_fields).to be >= (index.count_fields + index.sum_fields + index.avg_fields)
      end
    end

    it 'enumerates indexes with hash_fields that satisfy GROUP BY clause' do
      query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model
      indexes = enum.indexes_for_query query
      expect(indexes.any?{|i| i.hash_fields >= Set.new([tweet['Body']])}).to be(true)
      expect(indexes.any?{|i| i.order_fields.take(1).to_set == Set.new([tweet['Retweets']])}).to be(true)
    end

    it 'produce mv index for GROUP BY field which is part of eq conditions' do
      query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? AND Tweet.Retweets = ? GROUP BY Tweet.Retweets', workload.model
      mv_with_aggregation = query.materialize_view_with_aggregation
      expect(mv_with_aggregation.hash_fields).to eq(Set.new([tweet['Body']]))

      # since Cassandra does not allow GROUP BY on only a part of the partition key, GROUP BY should be the prefix of order_fields
      expect(mv_with_aggregation.order_fields.take(1).to_set).to eq(Set.new([tweet['Retweets']]))
    end

    it 'produce mv index for GROUP BY field which is part of range conditions' do
      query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets FROM Tweet WHERE ' \
                                'Tweet.Body = ? AND Tweet.Timestamp > ? GROUP BY Tweet.Retweets', workload.model
      mv_with_aggregation = query.materialize_view_with_aggregation
      expect(mv_with_aggregation.hash_fields).to eq(Set.new([tweet['Body']]))

      # since Cassandra does not allow GROUP BY on only a part of the partition key, GROUP BY should be the prefix of order_fields
      expect(mv_with_aggregation.order_fields.take(2).to_set).to eq(Set.new([tweet['Timestamp'], tweet['Retweets']]))
    end


    it 'produce index for query that uses composite key' do
      model = workload_composite_key.model
      workload_composite_key.add_statement Statement.parse 'SELECT Link.URL FROM Link.Tweets.User ' \
                      'WHERE User.Username = ? AND Tweets.TweetId = ? LIMIT 5', model
      enum = IndexEnumerator.new workload_composite_key
      indexes = enum.indexes_for_workload
      indexes.each do |index|
        if index.key_fields.include? model["Tweets"]["TweetId"]
          expect(index.key_fields.to_set).to be >= Set.new([model["Tweets"]["TweetId"], model["Tweets"]["FollowerId"]])
        end
      end
    end
  end
end
