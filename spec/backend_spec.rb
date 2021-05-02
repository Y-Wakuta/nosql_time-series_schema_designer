module NoSE
  module Backend
    describe Backend::SortStatementStep do
      include_context 'entities'

      it 'can sort a list of results' do
        results = [
          { 'User_Username' => 'Bob' },
          { 'User_Username' => 'Alice' }
        ]
        step = Plans::SortPlanStep.new [user['Username']]

        step_class = Backend::SortStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process nil, results

        expect(results).to eq [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
      end
    end

    describe Backend::FilterStatementStep do
      include_context 'entities'

      it 'can filter results by an equality predicate' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::FilterPlanStep.new [user['Username']], nil
        query = Statement.parse 'SELECT User.* FROM User ' \
                                'WHERE User.Username = "Bob"', workload.model

        step_class = Backend::FilterStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process query.conditions, results

        expect(results).to eq [
          { 'User_Username' => 'Bob' }
        ]
      end

      it 'can filter results by a range predicate' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::FilterPlanStep.new [], [user['Username']]
        query = Statement.parse 'SELECT User.* FROM User WHERE ' \
                                'User.Username < "B" AND ' \
                                'User.City = "New York"', workload.model

        step_class = Backend::FilterStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process query.conditions, results

        expect(results).to eq [
          { 'User_Username' => 'Alice' }
        ]
      end

      it 'can limit results' do
        results = [
          { 'User_Username' => 'Alice' },
          { 'User_Username' => 'Bob' }
        ]
        step = Plans::LimitPlanStep.new 1
        step_class = Backend::LimitStatementStep
        prepared = step_class.new nil, [], {}, step, nil, nil
        results = prepared.process({}, results)

        expect(results).to eq [
          { 'User_Username' => 'Alice' }
        ]
      end
    end

    describe Backend::AggregationStatementStep do
      include_context 'entities'

      it 'aggregates records on memory' do
        results = [
          { 'Tweet_TweetId' => 0, 'Tweet_Body' => 'tweet1', 'Tweet_Timestamp' => Time.new('2020-3-01'), 'Tweet_Retweets' => 0},
          { 'Tweet_TweetId' => 1, 'Tweet_Body' => 'tweet1', 'Tweet_Timestamp' => Time.new('2020-3-02'), 'Tweet_Retweets' => 0},
          { 'Tweet_TweetId' => 2, 'Tweet_Body' => 'tweet2', 'Tweet_Timestamp' => Time.new('2020-3-03'), 'Tweet_Retweets' => 1},
          { 'Tweet_TweetId' => 3, 'Tweet_Body' => 'tweet2', 'Tweet_Timestamp' => Time.new('2020-3-04'), 'Tweet_Retweets' => 1},
          { 'Tweet_TweetId' => 4, 'Tweet_Body' => 'tweet2', 'Tweet_Timestamp' => Time.new('2020-3-05'), 'Tweet_Retweets' => 2},
        ]
        step = Plans::AggregationPlanStep.new([tweet['TweetId']], [tweet['Retweets']], [], [tweet['Timestamp']], [tweet['Body']])
        step_class = Backend::AggregationStatementStep

        expected = [
          { 'Tweet_TweetId' => 2, 'Tweet_Body' => 'tweet1', 'Tweet_Timestamp' => Time.new('2020-3-02').to_f, 'Tweet_Retweets' => 0.0},
          { 'Tweet_TweetId' => 3, 'Tweet_Body' => 'tweet2', 'Tweet_Timestamp' => Time.new('2020-3-05').to_f, 'Tweet_Retweets' => 4.0},
        ]

        prepared = step_class.new nil, [], {}, step, nil, nil

        expect(prepared).to receive(:validate_all_field_aggregated?).and_return(true).exactly(expected.size).times
        actual = prepared.process({}, results)
        expect(actual).to eq expected

        # ==================================================================
        # check aggregation works for pre-aggregation on IndexLookupPlanStep
        # ==================================================================

        pre_aggregated_results = [
          { 'Tweet_TweetId' => 0, 'Tweet_Body' => 'tweet1', 'Tweet_Timestamp' => Time.new('2020-3-01'), 'Tweet_Retweets' => 0},
          { 'Tweet_TweetId' => 1, 'Tweet_Body' => 'tweet1', 'Tweet_Timestamp' => Time.new('2020-3-02'), 'Tweet_Retweets' => 0},
          { 'Tweet_TweetId' => 2, 'Tweet_Body' => 'tweet2', 'Tweet_Timestamp' => Time.new('2020-3-03'), 'Tweet_Retweets' => 1},
          { 'Tweet_TweetId' => 3, 'Tweet_Body' => 'tweet2', 'Tweet_Timestamp' => Time.new('2020-3-04'), 'Tweet_Retweets' => 1},
          { 'Tweet_TweetId' => 4, 'Tweet_Body' => 'tweet2', 'Tweet_Timestamp' => Time.new('2020-3-05'), 'Tweet_Retweets' => 2},
        ]

        expect(prepared).to receive(:validate_all_field_aggregated?).and_return(true).exactly(expected.size).times
        actual = prepared.process({}, pre_aggregated_results)
        expect(actual).to eq expected
      end
    end
  end
end
