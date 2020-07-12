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
        expect(query.groupby).to be > simplified_query.groupby
        expect(query.conditions).to be > simplified_query.conditions
      end
    end
  end
end
