module NoSE
  module Cost
    describe Cost do
      it 'should register all subclasses' do
        expect(Cost.subclasses).to have_key 'NoSE::Cost::RequestCountCost'
        expect(Cost.subclasses).to have_key 'NoSE::Cost::EntityCountCost'
        expect(Cost.subclasses).to have_key 'NoSE::Cost::FieldSizeCost'
      end
    end

    describe RequestCountCost do
      include_context 'entities'

      it 'is a type of cost' do
        expect(RequestCountCost.subtype_name).to eq 'request_count'
      end

      it 'counts a single request for a single step plan' do
        planner = Plans::QueryPlanner.new workload.model,
                                          [tweet.simple_index], subject
        plan = planner.min_plan \
          Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                          'WHERE Tweet.TweetId = ?', workload.model
        expect(plan.cost).to eq 1
      end
    end

    describe EntityCountCost do
      include_context 'entities'

      it 'is a type of cost' do
        expect(EntityCountCost.subtype_name).to eq 'entity_count'
      end

      it 'counts multiple requests when multiple entities are selected' do
        query = Statement.parse 'SELECT Tweet.* FROM Tweet.User ' \
                                'WHERE User.UserId = ?', workload.model
        planner = Plans::QueryPlanner.new workload.model,
                                          [query.materialize_view], subject
        plan = planner.min_plan query
        expect(plan.cost).to eq 100
      end
    end

    describe FieldSizeCost do
      include_context 'entities'

      it 'is a type of cost' do
        expect(FieldSizeCost.subtype_name).to eq 'field_size'
      end

      it 'measures the size of the selected data' do
        index = tweet.simple_index
        planner = Plans::QueryPlanner.new workload.model, [index], subject
        plan = planner.min_plan \
          Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                          'WHERE Tweet.TweetId = ?', workload.model
        expect(plan.cost).to eq index.all_fields.sum_by(&:size)
      end
    end

    describe 'DummyCost' do
      include_context 'entities'
      include_context 'dummy cost model'

      it 'increase the cost if the index include aggregation field' do
        query = Statement.parse 'SELECT Tweet.Body, Tweet.Timestamp, Tweet.Retweets, count(Tweet.TweetId) FROM Tweet ' \
                          'WHERE Tweet.TweetId = ?', workload.model
        index = Index.new [tweet['TweetId']], [tweet['Body']],
                          [tweet['Timestamp'], tweet['Retweets']],
                          QueryGraph::Graph.from_path(
                              [tweet.id_field])
        planner = Plans::QueryPlanner.new workload.model, [index], cost_model
        plan = planner.min_plan query

        query = Statement.parse 'SELECT Tweet.* FROM Tweet ' \
                          'WHERE Tweet.TweetId = ?', workload.model
        simple_index = tweet.simple_index
        planner = Plans::QueryPlanner.new workload.model, [simple_index], cost_model
        simple_plan = planner.min_plan query

        expect(plan.cost).to be > simple_plan.cost
      end
    end
  end
end
