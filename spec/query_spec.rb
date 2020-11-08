module NoSE
  describe Query do
    include_context 'entities'

    it 'is reproduced by its text' do
      query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, count(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model
      reproduced_query = Statement.parse query.unparse, workload.model
      expect(query).to be == reproduced_query
    end

    it 'enumerates simplified queries' do
      query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, count(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? AND Tweet.Timestamp = ? GROUP BY Tweet.Retweets, Tweet.TweetId', workload.model
      query.simplified_queries.each do |simplified_query|
        expect(query.select).to be > simplified_query.select
        expect(query.conditions).to be > simplified_query.conditions
      end
    end
  end

  describe MigrateSupportSimplifiedQuery do
    include_context 'entities'
    it 'simplifies query' do
      query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, count(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? AND Tweet.Retweets > ? GROUP BY Tweet.Retweets', workload.model
      simplified_query = MigrateSupportSimplifiedQuery.simple_query query, query.materialize_view
      deserialized_query = Statement.parse simplified_query.text, workload.model
      expected_query = Statement.parse 'SELECT Tweet.TweetId, Tweet.Retweets, Tweet.Timestamp FROM Tweet WHERE ' \
                                'Tweet.Body = ?', workload.model
      expect(deserialized_query).to eq(expected_query)
    end
  end
end
