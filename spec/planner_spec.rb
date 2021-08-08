module NoSE
  module Plans
    describe QueryPlanner do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'can look up fields by key' do
        index = tweet.simple_index
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet ' \
                                'WHERE Tweet.TweetId = ?', workload.model

        tree = planner.find_plans_for_query query
        expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
        expect(tree).to have(1).plan
        expect(tree.first.cost).to be > 0
      end

      it 'does not use an index with the wrong key path' do
        query = Statement.parse 'SELECT User.Username FROM Tweet.User' \
                                ' WHERE Tweet.TweetId = ?', workload.model
        good_index = query.materialize_view
        bad_index = good_index.dup
        path = KeyPath.new [user.id_field, user['Favourite']]
        bad_index.instance_variable_set :@path, path
        bad_index.instance_variable_set :@graph,
                                        QueryGraph::Graph.from_path(path)

        # With the correct path, this should work
        planner = QueryPlanner.new workload.model, [good_index], cost_model
        expect { planner.find_plans_for_query query }.not_to raise_error

        # With the wrong path, this should fail
        planner = QueryPlanner.new workload.model, [bad_index], cost_model
        expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
      end

      it 'can perform an external sort if an index does not exist' do
        index = Index.new [user['City']], [user['UserId'], tweet['TweetId']],
                          [tweet['Timestamp'], tweet['Body']],
                          QueryGraph::Graph.from_path(
                            [user.id_field, user['Tweets']]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.City = ? ORDER BY Tweet.Timestamp',
                                workload.model

        tree = planner.find_plans_for_query query
        steps = [
          IndexLookupPlanStep.new(index),
          SortPlanStep.new([tweet['Timestamp']])
        ]
        steps.each { |step| step.calculate_cost cost_model }
        expect(tree.first).to eq steps
        expect(tree).to have(1).plan
      end

      it 'can sort if data on all entities has been fetched' do
        index1 = Index.new [user['UserId']], [tweet['TweetId']],
                           [user['Username']],
                           QueryGraph::Graph.from_path(
                             [user.id_field, user['Tweets']]
                           )
        index2 = Index.new [tweet['TweetId']], [], [tweet['Body']],
                           QueryGraph::Graph.from_path([tweet.id_field])
        planner = QueryPlanner.new workload.model, [index1, index2], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User WHERE ' \
                                'User.UserId = ? ORDER BY User.Username',
                                workload.model
        expect(planner.min_plan(query)).to eq [
                                                IndexLookupPlanStep.new(index1),
                                                SortPlanStep.new([user['Username']]),
                                                IndexLookupPlanStep.new(index2)
                                              ]
      end

      it 'can apply a limit directly' do
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.UserId = ? LIMIT 5', workload.model
        index = query.materialize_view
        planner = QueryPlanner.new workload.model, [index], cost_model

        tree = planner.find_plans_for_query query
        expect(tree.first).to eq([IndexLookupPlanStep.new(index)])
        expect(tree).to have(1).plan
        expect(tree.first.last.state.cardinality).to eq 5
      end

      it 'can perform an external sort followed by a limit' do
        index = Index.new [user['UserId']], [tweet['TweetId']],
                          [tweet['Timestamp'], tweet['Body']],
                          QueryGraph::Graph.from_path(
                            [user.id_field, user['Tweets']]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.UserId = ? ORDER BY ' \
                                'Tweet.Timestamp LIMIT 5', workload.model

        tree = planner.find_plans_for_query query
        steps = [
          IndexLookupPlanStep.new(index),
          SortPlanStep.new([tweet['Timestamp']]),
          LimitPlanStep.new(5)
        ]
        steps.each { |step| step.calculate_cost cost_model }
        expect(tree.first).to eq steps
        expect(tree).to have(1).plan
      end

      it 'raises an exception if there is no plan' do
        planner = QueryPlanner.new workload.model, [], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet ' \
                                'WHERE Tweet.TweetId = ?', workload.model
        expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
      end

      it 'can find multiple plans' do
        index1 = Index.new [user['UserId']],
                           [tweet['Timestamp'], tweet['TweetId']],
                           [tweet['Body']],
                           QueryGraph::Graph.from_path(
                             [user.id_field, user['Tweets']]
                           )
        index2 = Index.new [user['UserId']], [tweet['TweetId']],
                           [tweet['Timestamp'], tweet['Body']],
                           QueryGraph::Graph.from_path(
                             [user.id_field, user['Tweets']]
                           )
        planner = QueryPlanner.new workload.model, [index1, index2], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User WHERE ' \
                                'User.UserId = ? ORDER BY Tweet.Timestamp',
                                workload.model

        tree = planner.find_plans_for_query query
        expect(tree.to_a).to match_array [
                                           [IndexLookupPlanStep.new(index1)],
                                           [
                                             IndexLookupPlanStep.new(index2),
                                             SortPlanStep.new([tweet['Timestamp']])
                                           ]
                                         ]
      end

      it 'knows which fields are available at a given step' do
        index = Index.new [tweet['TweetId']], [],
                          [tweet['Body'], tweet['Timestamp']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet ' \
                                'WHERE Tweet.TweetId = ?', workload.model

        plan = planner.find_plans_for_query(query).first
        expect(plan.last.fields).to include(tweet['TweetId'], tweet['Body'],
                                            tweet['Timestamp'])
      end

      it 'can apply external filtering' do
        index = Index.new [tweet['TweetId']], [],
                          [tweet['Body'], tweet['Timestamp']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field]
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet WHERE ' \
                                'Tweet.TweetId = ? AND Tweet.Timestamp > ?',
                                workload.model

        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        expect(tree.first.last).to eq FilterPlanStep.new([],
                                                         tweet['Timestamp'])
      end

      it 'can apply an index lookup step that includes count()' do
        index = Index.new [tweet['TweetId']], [],
                          [tweet['Body']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field]
                          ), count_fields: Set.new([tweet['Body']])
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT count(Tweet.Body) FROM Tweet WHERE ' \
                                'Tweet.TweetId = ?', workload.model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        expect(tree.first).to have(1).steps
      end

      it 'creates Aggregation step for the last step of query plan' do
        parent_index = Index.new [tweet['Body']], [tweet['TweetId']],
                                 [],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 )
        index = Index.new [tweet['TweetId']], [],
                          [tweet['Timestamp']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field])
        planner = QueryPlanner.new workload.model, [parent_index, index], cost_model
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Timestamp FROM Tweet WHERE ' \
                                'Tweet.Body = ?', workload.model
        plan = planner.find_plans_for_query(query).first
        plan.steps[0..-2].each do |s|
          expect(s.class).not_to be AggregationPlanStep
        end
        expect(plan.steps.last.class).to be AggregationPlanStep
      end

      it 'can apply aggregation function at last step in the query plan' do
        parent_index = Index.new [tweet['Body']], [tweet['TweetId'], tweet['Retweets']],
                                 [],
                                 QueryGraph::Graph.from_path(
                                     [tweet.id_field]
                                 )
        index = Index.new [tweet['TweetId']], [],
                          [tweet['Timestamp'], tweet['Retweets']],
                          QueryGraph::Graph.from_path(
                              [tweet.id_field]), count_fields: Set.new([tweet['TweetId']]),
                          sum_fields: Set.new([tweet['Retweets']]), avg_fields: Set.new([tweet['Timestamp']])
        planner = QueryPlanner.new workload.model, [parent_index, index], cost_model
        query = Statement.parse 'SELECT count(Tweet.TweetId), sum(Tweet.Retweets), avg(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ?', workload.model

        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan

        last_index = tree.first.steps.last.index
        expect(last_index.sum_fields).to include Set.new([tweet['Retweets']])
        expect(last_index.count_fields).to include Set.new([tweet['TweetId']])
        expect(last_index.avg_fields).to include Set.new([tweet['Timestamp']])
      end

      it 'can apply group by in the query on database' do
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model
        parent_index = Index.new [tweet['Body']], [tweet['TweetId']],
                                 [tweet['Retweets']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 )
        index = Index.new  [tweet['TweetId']], [tweet['Retweets']],
                           [tweet['Timestamp']],
                           QueryGraph::Graph.from_path(
                             [tweet.id_field]), count_fields: Set.new([tweet['TweetId']]),
                           sum_fields: Set.new([tweet['Timestamp']]), groupby_fields: Set.new([tweet['Retweets']])
        planner = QueryPlanner.new workload.model, [parent_index, index], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        expect(tree.first.steps.last.class).to be IndexLookupPlanStep
        expect(tree.first.steps.last.index.count_fields).to eq(Set.new([tweet['TweetId']]))
        expect(tree.first.steps.last.index.sum_fields).to eq(Set.new([tweet['Timestamp']]))
        expect(tree.first.steps.last.index.groupby_fields).to eq(Set.new([tweet['Retweets']]))
      end

      it 'does not apply partial aggregation' do
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model
        index = Index.new [tweet['Body']], [tweet['Retweets'], tweet['TweetId']],
                                 [tweet['Timestamp']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 ), count_fields: Set.new([tweet['TweetId']]),
                           sum_fields: Set.new([tweet['Timestamp']]), groupby_fields: Set.new([tweet['Retweets']])
        planner = QueryPlanner.new workload.model, [index], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan

        index_without_count = Index.new [tweet['Body']], [tweet['Retweets'], tweet['TweetId']],
                                 [tweet['Timestamp']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 ), sum_fields: Set.new([tweet['Timestamp']]), groupby_fields: Set.new([tweet['Retweets']])
        planner_ = QueryPlanner.new workload.model, [index_without_count], cost_model
        expect do
          planner_.find_plans_for_query(query)
        end.to raise_error(NoPlanException)
      end

      it 'applies aggregation before sort step' do
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? ORDER BY Tweet.Retweets, Tweet.Timestamp GROUP BY Tweet.Retweets', workload.model
        index = Index.new [tweet['Body']], [tweet['Retweets'], tweet['TweetId']],
                                 [tweet['Timestamp']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 )
        planner = QueryPlanner.new workload.model, [index], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        steps = tree.first.steps
        expect(steps.index{|s| s.instance_of?(AggregationPlanStep)}).to be < steps.index{|s| s.instance_of?(SortPlanStep)}
      end

      it 'applies aggregation and sort to the same field' do
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? ORDER BY Tweet.Retweets GROUP BY Tweet.Retweets', workload.model
        index = Index.new [tweet['Body']], [tweet['Retweets'], tweet['TweetId']],
                                 [tweet['Timestamp']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 )
        planner = QueryPlanner.new workload.model, [index], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        steps = tree.first.steps
        expect(steps.index{|s| s.instance_of?(AggregationPlanStep)}).to be < steps.index{|s| s.instance_of?(SortPlanStep)}
      end

      it 'does not apply Filter after aggregation on IndexLookupPlanStep' do
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? AND Tweet.TweetId = ? GROUP BY Tweet.Retweets', workload.model
        index = Index.new [tweet['Body']], [tweet['Retweets'], tweet['TweetId']],
                         [tweet['Timestamp']],
                         QueryGraph::Graph.from_path(
                           [tweet.id_field]
                         ), count_fields: Set.new([tweet['TweetId']]),
                   sum_fields: Set.new([tweet['Timestamp']]), groupby_fields: Set.new([tweet['Retweets']])
        planner = QueryPlanner.new workload.model, [index], cost_model
        expect do
          planner.find_plans_for_query(query)
        end.to raise_error(NoPlanException)

        index = Index.new [tweet['Body']], [tweet['Retweets'], tweet['TweetId']],
                 [tweet['Timestamp']],
                 QueryGraph::Graph.from_path(
                   [tweet.id_field]
                 )
        planner = QueryPlanner.new workload.model, [index], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        steps = tree.first.steps
        indexlookupsteps_before_filter = steps[0...steps.index{|s| s.instance_of?(FilterPlanStep)}].select{|s| s.instance_of?(IndexLookupPlanStep)}
        expect(indexlookupsteps_before_filter.any?{|ibf| ibf.index.has_aggregation_fields?}).to be false
      end

      it 'uses CF that does aggregation with range condition and then apply AggregationPlanStep' do
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? AND Tweet.TweetId > 0 GROUP BY Tweet.Retweets', workload.model
        index = query.materialize_view_with_aggregation
        #index = Index.new [tweet['Body']], [ tweet['TweetId'], tweet['Retweets']],
        #                         [tweet['Timestamp']],
        #                         QueryGraph::Graph.from_path(
        #                           [tweet.id_field]
        #                         ), count_fields: Set.new([tweet['TweetId']]),
        #                   sum_fields: Set.new([tweet['Timestamp']]), groupby_fields: Set.new([tweet['TweetId'], tweet['Retweets']]), extra_groupby_fields: Set.new([tweet['TweetId']])
        planner = QueryPlanner.new workload.model, [index], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan

        index_without_count = Index.new [tweet['Body']], [tweet['Retweets'], tweet['TweetId']],
                                 [tweet['Timestamp']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 ), sum_fields: Set.new([tweet['Timestamp']]), groupby_fields: Set.new([tweet['Retweets']])
        planner_ = QueryPlanner.new workload.model, [index_without_count], cost_model
        expect do
          planner_.find_plans_for_query(query)
        end.to raise_error(NoPlanException)
      end

      it 'can apply group by in the query on memory' do
        query = Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, sum(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model
        parent_index = Index.new [tweet['Body']], [tweet['TweetId']],
                                 [tweet['Retweets']],
                                 QueryGraph::Graph.from_path(
                                   [tweet.id_field]
                                 )
        index = Index.new  [tweet['TweetId']], [tweet['Retweets']],
                           [tweet['Timestamp']],
                           QueryGraph::Graph.from_path(
                             [tweet.id_field])
        planner = QueryPlanner.new workload.model, [parent_index, index], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        expect(tree.first.steps.last.class).to be AggregationPlanStep
      end

      it 'can apply composite key in query planning' do
        workload_comp = workload_composite_key
        user_comp = workload_comp.model["User"]
        tweet_comp = workload_comp.model["Tweets"]
        model = workload_comp.model
        query =  Statement.parse 'SELECT Tweets.Body, Tweets.Timestamp FROM Tweets.User ' \
                      'WHERE User.Username = ? AND Tweets.TweetId = ? LIMIT 5', model
        workload_comp.add_statement query

        #============================
        # composite key should be included
        #============================
        single_parent_index = Index.new [user_comp['Username']],
                                        [user_comp['UserId'], tweet_comp['TweetId']], [],
                                        QueryGraph::Graph.from_path([user_comp.id_field, user_comp['Tweets']])
        single_last_index = Index.new [tweet_comp['TweetId']] , [],
                                      [tweet_comp['Body'], tweet_comp['Timestamp']], QueryGraph::Graph.from_path([tweet_comp.id_field])
        planner = QueryPlanner.new workload_comp.model, [single_parent_index, single_last_index], cost_model
        # join should use all of composite key to join CF
        expect { planner.find_plans_for_query query }.to raise_error NoPlanException

        #============================
        # composite key should be included in hash_field or prefix order_fields
        #============================
        composite_parent_index = Index.new [user_comp['Username']],
                                           [user_comp['UserId'], tweet_comp['TweetId'], tweet_comp['FollowerId']], [],
                                           QueryGraph::Graph.from_path([user_comp.id_field, user_comp['Tweets']])
        composite_last_index = Index.new [tweet_comp['TweetId']] , [tweet_comp['Body'],tweet_comp['FollowerId']],
                                         [ tweet_comp['Timestamp']], QueryGraph::Graph.from_path([tweet_comp.id_field])
        planner = QueryPlanner.new workload_comp.model, [composite_parent_index, composite_last_index], cost_model
        expect { planner.find_plans_for_query query }.to raise_error NoPlanException

        #============================
        # primary key and composite key can exist both in hash_field and prefix order_field
        #============================
        composite_parent_index = Index.new [user_comp['Username']],
                                           [user_comp['UserId'], tweet_comp['TweetId'], tweet_comp['FollowerId']], [],
                                           QueryGraph::Graph.from_path([user_comp.id_field, user_comp['Tweets']])
        composite_last_index1 = Index.new [tweet_comp['TweetId']] , [tweet_comp['FollowerId']],
                                          [tweet_comp['Body'], tweet_comp['Timestamp']], QueryGraph::Graph.from_path([tweet_comp.id_field])
        composite_last_index2 = Index.new [tweet_comp['TweetId'], tweet_comp['FollowerId']], [],
                                          [tweet_comp['Body'], tweet_comp['Timestamp']], QueryGraph::Graph.from_path([tweet_comp.id_field])
        planner = QueryPlanner.new workload_comp.model, [composite_parent_index, composite_last_index1, composite_last_index2], cost_model
        tree = planner.find_plans_for_query(query)
        expect(tree.to_a.size).to be 2
      end

      it 'can select part of composite key' do
        workload_comp = workload_composite_key
        tweet_comp = workload_comp.model["Tweets"]
        model = workload_comp.model
        query =  Statement.parse 'SELECT Tweets.FollowerId FROM Tweets ' \
                      'WHERE Tweets.TweetId = ?', model
        workload_comp.add_statement query

        single_last_index = Index.new [tweet_comp['TweetId']] , [tweet_comp['FollowerId']],
                                      [tweet_comp['Body']], QueryGraph::Graph.from_path([tweet_comp.id_field])
        planner = QueryPlanner.new workload_comp.model, [single_last_index], cost_model
        plan =  planner.min_plan query

        # If the plan is MV plan, that should not have composite key.
        # As long as the query does not specify composite key, composite key should no specified for the first step
        expect(plan.steps.first.eq_filter).to eq(Set.new([tweet_comp['TweetId']]))
      end

      context 'when updating cardinality' do
        before(:each) do
          simple_query = Statement.parse 'SELECT Tweet.Body FROM ' \
                                         'Tweet WHERE Tweet.TweetId = ?',
                                         workload.model
          @simple_state = QueryState.new simple_query, workload.model

          # Pretend we start with all tweets
          @simple_state.cardinality = tweet.count

          query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                  'WHERE User.UserId = ?', workload.model
          @state = QueryState.new query, workload.model
        end

        it 'can reduce the cardinality to 1 when filtering by ID' do
          step = FilterPlanStep.new [tweet['TweetId']], nil, @simple_state
          expect(step.state.cardinality).to eq 1
        end

        it 'can apply equality predicates when filtering' do
          step = FilterPlanStep.new [tweet['Body']], nil, @simple_state
          expect(step.state.cardinality).to eq 200
        end

        it 'can apply multiple predicates when filtering' do
          step = FilterPlanStep.new [tweet['Body']], tweet['Timestamp'],
                                    @simple_state
          expect(step.state.cardinality).to eq 20
        end

        it 'can apply range predicates when filtering' do
          step = FilterPlanStep.new [], tweet['Timestamp'], @simple_state
          expect(step.state.cardinality).to eq 100
        end

        it 'can update the cardinality when performing a lookup' do
          index = Index.new [user['UserId']], [tweet['TweetId']],
                            [tweet['Body']],
                            QueryGraph::Graph.from_path(
                              [user.id_field, user['Tweets']]
                            )
          step = IndexLookupPlanStep.new index, @state,
                                         RootPlanStep.new(@state)
          expect(step.state.cardinality).to eq 100
        end
      end

      it 'fails if required fields are not available' do
        indexes = [
          Index.new([user['Username']], [user['UserId']], [user['City']],
                    QueryGraph::Graph.from_path([user.id_field])),
          Index.new([tweet['TweetId']], [], [tweet['Body']],
                    QueryGraph::Graph.from_path([tweet.id_field]))
        ]
        planner = QueryPlanner.new workload.model, indexes, cost_model
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = ?', workload.model
        expect { planner.find_plans_for_query query }.to \
          raise_error NoPlanException
      end

      it 'can use materialized views which traverse multiple entities' do
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = ?', workload.model
        workload.add_statement query
        indexes = IndexEnumerator.new(workload).indexes_for_workload

        planner = QueryPlanner.new workload.model, indexes, cost_model
        plans = planner.find_plans_for_query(query)
        plan_indexes = plans.map(&:indexes)

        expect(plan_indexes).to include [query.materialize_view]
      end

      it 'can use multiple indices for a query' do
        query = Statement.parse 'SELECT Tweet.Body FROM Tweet.User ' \
                                'WHERE User.Username = ?', workload.model
        workload.add_statement query

        indexes = [
          Index.new([user['Username']],
                    [user['UserId'], tweet['TweetId']], [],
                    QueryGraph::Graph.from_path([user.id_field,
                                                 user['Tweets']])),
          Index.new([tweet['TweetId']], [], [tweet['Body']],
                    QueryGraph::Graph.from_path([tweet.id_field]))
        ]

        planner = QueryPlanner.new workload.model, indexes, cost_model
        expect(planner.min_plan(query)).to eq [
                                                IndexLookupPlanStep.new(indexes[0]),
                                                IndexLookupPlanStep.new(indexes[1])
                                              ]
      end

      it 'can create plans which visit each entity' do
        query = Statement.parse 'SELECT Link.URL FROM Link.Tweets.User ' \
                                'WHERE User.Username = ?', workload.model
        workload.add_statement query

        indexes = IndexEnumerator.new(workload).indexes_for_workload
        planner = QueryPlanner.new workload.model, indexes, cost_model

        tree = planner.find_plans_for_query(query)
        max_steps = tree.max_by(&:length).length
        expect(max_steps).to be >= query.key_path.length + 1
      end

      it 'does not use limits for a single entity result set' do
        query = Statement.parse 'SELECT User.* FROM User ' \
                                'WHERE User.UserId = ? ' \
                                'ORDER BY User.Username LIMIT 10', workload.model
        workload.add_statement query

        indexes = IndexEnumerator.new(workload).indexes_for_workload
        planner = QueryPlanner.new workload.model, indexes, cost_model
        plans = planner.find_plans_for_query query

        expect(plans).not_to include(a_kind_of(LimitPlanStep))
      end

      it 'uses implicit sorting when the clustering key is filtered' do
        query = Statement.parse 'SELECT Tweets.Body FROM User.Tweets WHERE ' \
                                'User.UserId = ? AND Tweets.Retweets = 0 ' \
                                'ORDER BY Tweets.Timestamp', workload.model
        index = Index.new [user['UserId']], [tweet['Retweets'],
                                             tweet['Timestamp'], tweet['TweetId']],
                          [tweet['Body']],
                          QueryGraph::Graph.from_path(
                            [user.id_field, user['Tweets']]
                          )

        planner = QueryPlanner.new workload.model, [index], cost_model
        plan = planner.min_plan query

        expect(plan.steps).not_to include(a_kind_of(SortPlanStep))
      end

      it 'enumerates join query plans for complicated queries' do
        tpch_workload = Workload.new do |_| Model('tpch')
          Group 'Group1', default: 1 do
            Q 'SELECT l_orderkey.o_orderkey, sum(lineitem.l_extendedprice), sum(lineitem.l_discount), l_orderkey.o_orderdate, l_orderkey.o_shippriority '\
              'FROM lineitem.l_orderkey.o_custkey '\
              'WHERE o_custkey.c_mktsegment = ? AND lineitem.l_shipdate > ? '\
              'ORDER BY lineitem.l_extendedprice, lineitem.l_discount, l_orderkey.o_orderdate ' \
              'GROUP BY l_orderkey.o_orderkey, l_orderkey.o_orderdate, l_orderkey.o_shippriority -- Q3'
          end
        end

        indexes = GraphBasedIndexEnumerator.new(tpch_workload, cost_model, 2, 1_000).indexes_for_workload
        planner = QueryPlanner.new tpch_workload.model, indexes, cost_model
        tpch_workload.statement_weights.select{|s| s.instance_of? Query}.keys.each do |q|
          join_plans = planner.find_plans_for_query(q).select do |plan|
            index_lookup_steps = plan.steps.select{|s| s.is_a? Plans::IndexLookupPlanStep}
            index_lookup_steps.size > 1
          end
          expect(join_plans.size).to be > 0
        end
      end

      it 'fields in clustering key and ORDER BY matches' do
        tpch_workload = Workload.new do |_| Model('tpch')
        Group 'Group1', default: 1 do
          Q 'SELECT ps_suppkey.s_acctbal, ps_suppkey.s_name, s_nationkey.n_name, ps_suppkey.s_phone ' \
              'FROM part.from_partsupp.ps_suppkey.s_nationkey ' \
              'WHERE part.p_size = ? AND part.p_type = ? '\
              'ORDER BY ps_suppkey.s_acctbal, s_nationkey.n_name, ps_suppkey.s_name -- Q2_outer'
        end
        end

        indexes = GraphBasedIndexEnumerator.new(tpch_workload, cost_model,
                                                3, 1_000).indexes_for_workload
        planner = QueryPlanner.new tpch_workload.model, indexes, cost_model
        tpch_workload.statement_weights.select{|s| s.instance_of? Query}.keys.each do |q|
          tree = planner.find_plans_for_query(q)
          tree.each do |plan|
            plan.each do |step|
              next unless step.instance_of?(Plans::IndexLookupPlanStep)
              next if step.order_by.empty?
              step_order_by = step.index.hash_fields.to_a + step.index.order_fields
              expect(step_order_by.drop_while{|o| (step.eq_filter - step.order_by).include? o}.take(step.order_by.size)).to be == step.order_by
            end
          end
          expect(tree.size).to be > 0
        end
      end
    end

    describe PrunedQueryPlanner do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'queries share indexes' do
        tpch_workload = Workload.new do |_|
          Model 'tpch'
          Group 'Group1', default: 1 do
            Q 'SELECT c_nationkey.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
              'FROM lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
              'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? ' \
              'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
              'GROUP BY c_nationkey.n_name -- Q5'

            Q 'SELECT l_orderkey.o_orderdate, sum(from_lineitem.l_extendedprice), sum(from_lineitem.l_discount) '\
              'FROM part.from_partsupp.from_lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
              'WHERE c_nationkey.n_name = ? AND n_regionkey.r_name = ? AND part.p_type = ? AND l_orderkey.o_orderdate < ? ' \
              'ORDER BY l_orderkey.o_orderdate ' \
              'GROUP BY l_orderkey.o_orderdate -- Q8'
          end
        end
        indexes = GraphBasedIndexEnumerator.new(tpch_workload, cost_model, 3, 1_000).indexes_for_workload.to_a
        pruned_planner = QueryPlanner.new tpch_workload.model, indexes, cost_model
        query_indexes_hash = tpch_workload.statement_weights.keys.flat_map do |q|
          pruned_plan = pruned_planner.find_plans_for_query q
          plan_indexes = pruned_plan.map{|p| p.select{|s| s.is_a? Plans::IndexLookupPlanStep}.map{|s| s.index}}.flatten
          Hash[q, plan_indexes]
        end.inject(&:merge)

        shared_indexes = query_indexes_hash.values.first & query_indexes_hash.values.last
        expect(shared_indexes.size).to be 20
      end

      it 'enumerates only allowed depth query plan' do
        tpch_workload = Workload.new do |_|
          Model 'tpch'
          Group 'Group1', default: 1 do
            Q 'SELECT c_nationkey.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
              'FROM lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
              'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate < ? ' \
              'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
              'GROUP BY c_nationkey.n_name -- Q5'
          end
        end
        1.upto(3).each do |index_step_size_threshold|
          indexes = GraphBasedIndexEnumerator.new(tpch_workload, cost_model,
                                              index_step_size_threshold, 1000)
                                         .indexes_for_workload.to_a
          pruned_planner = PrunedQueryPlanner.new tpch_workload.model, indexes, cost_model, index_step_size_threshold
          tpch_workload.statement_weights.keys.select { |s| s.instance_of? Query}.each do |q|
            step_sizes = pruned_planner.find_plans_for_query(q).map do |plan|
              index_steps = plan.select{|s| s.is_a? Plans::IndexLookupPlanStep}
              expect(index_steps.size).to be <= index_step_size_threshold
              index_steps.size
            end
            # some of them should have many step join plan if it is allowed
            expect(step_sizes.max()).to be >= index_step_size_threshold - 1
          end
        end
      end
    end

    describe MigrateSupportSimpleQueryPlanner do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'only has ExtractSteps' do
        tpch_workload = TimeDependWorkload.new do |_|
          TimeSteps 3
          Model 'tpch'
          Group 'Group1', default: [1,2,3] do
            Q 'SELECT customer.c_phone, sum(customer.c_acctbal), count(customer.c_custkey) ' \
              'FROM customer ' \
              'WHERE customer.c_phone = ? AND customer.c_custkey = ? AND customer.c_acctbal > ? ' \
              'ORDER BY customer.c_phone ' \
              'GROUP BY customer.c_phone -- Q22'
          end
        end
        indexes = IndexEnumerator.new(tpch_workload).indexes_for_workload [], false
        indexes.each do |base_index|
          query = MigrateSupportSimplifiedQuery.simple_query(tpch_workload.statement_weights.keys.first, base_index)
          planner = Plans::MigrateSupportSimpleQueryPlanner.new tpch_workload, indexes, cost_model, 2
          search = Search::Search.new(tpch_workload, cost_model)
          tree = search.send(:support_query_cost, query, planner)[:tree]
          tree.each do |plan|
            plan.each do |step|
              expect(step.class).to be ExtractPlanStep
            end
          end
        end
      end

      it 'pruduce prepare plans for CF that has aggregation' do
        target_index = Index.new [td_tweet['TweetId']], [td_tweet['Body']],
                  [td_tweet['Timestamp'], td_tweet['Retweets']],
                  QueryGraph::Graph.from_path(
                      [td_tweet.id_field]), count_fields: Set.new([td_tweet['TweetId']])

        index1 = Index.new [td_tweet['TweetId']], [td_tweet['Body'], td_tweet['Timestamp']],
                                 [td_tweet['Retweets']],
                                 QueryGraph::Graph.from_path(
                                   [td_tweet.id_field]), count_fields: Set.new([td_tweet['TweetId']])

        index2 = Index.new [td_tweet['TweetId']], [td_tweet['Body']],
                                 [td_tweet['Timestamp'], td_tweet['Retweets']],
                                 QueryGraph::Graph.from_path(
                                   [td_tweet.id_field])

        migrate_support_query = MigrateSupportQuery.migrate_support_query_for_index(target_index)
        planner = Plans::MigrateSupportSimpleQueryPlanner.new td_workload, [index1, index2], cost_model, 2
        search = Search::Search.new(td_workload, cost_model)
        tree = search.send(:support_query_cost, migrate_support_query, planner)[:tree]
        expect(tree.to_a.size).to be 2
        expect(tree.flat_map(&:indexes).to_set).to eq(Set.new([index1, index2]))
      end
    end

    describe UpdatePlanner do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'can produce a simple plan for an update' do
        update = Statement.parse 'UPDATE User SET City = ? ' \
                                 'WHERE User.UserId = ?', workload.model
        index = Index.new [tweet['Timestamp']],
                          [tweet['TweetId'], user['UserId']], [user['City']],
                          QueryGraph::Graph.from_path(
                            [tweet.id_field, tweet['User']]
                          )
        workload.add_statement update
        indexes = IndexEnumerator.new(workload).indexes_for_workload [index]
        planner = Plans::QueryPlanner.new workload.model, indexes, cost_model

        trees = update.support_queries(index).map do |query|
          planner.find_plans_for_query(query)
        end
        planner = UpdatePlanner.new workload.model, trees, cost_model
        plans = planner.find_plans_for_update update, indexes
        plans.each { |plan| plan.select_query_plans indexes }

        update_steps = [
          InsertPlanStep.new(index)
        ]
        plan = UpdatePlan.new update, index, trees, update_steps, cost_model
        plan.select_query_plans indexes
        expect(plans).to match_array [plan]
      end

      it 'can produce a plan with no support queries' do
        update = Statement.parse 'UPDATE User SET City = ? ' \
                                 'WHERE User.UserId = ?', workload.model
        index = Index.new [user['UserId']], [], [user['City']],
                          QueryGraph::Graph.from_path([user.id_field])
        planner = UpdatePlanner.new workload.model, [], cost_model
        plans = planner.find_plans_for_update update, [index]
        plans.each { |plan| plan.select_query_plans [index] }

        expect(plans).to have(1).item
        expect(plans.first.query_plans).to be_empty
      end

      #it 'produces support query for single-entity query' do
      #  update = Statement.parse 'INSERT INTO User SET UserId = ?, City = ?, Username = ?' , workload.model
      #  index = Index.new [user['Username']],
      #                    [user['Country'], user['UserId']], [user['City']],
      #                    QueryGraph::Graph.from_path(
      #                      [user.id_field]
      #                    )

      #  queries = update.support_queries(index)
      #  expect(queries.size).to be > 0
      #end
    end
  end
end
