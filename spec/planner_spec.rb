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
                          )
        planner = QueryPlanner.new workload.model, [index], cost_model
        query = Statement.parse 'SELECT count(Tweet.Body) FROM Tweet WHERE ' \
                                'Tweet.TweetId = ?', workload.model
        tree = planner.find_plans_for_query(query)
        expect(tree).to have(1).plan
        expect(tree.first).to have(2).steps
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

      it 'can apply group by in the query' do
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
            Q 'INSERT INTO orders SET o_orderkey=?, o_orderstatus=?, o_totalprice=?, o_orderdate=?, o_orderpriority=?, '\
                  'o_clerk=?, o_shippriority=?, o_comment=? AND CONNECT TO to_customer(?) -- 4'
            Q 'SELECT lineitem.l_orderkey, lineitem.l_extendedprice, to_orders.o_shippriority '\
                'FROM lineitem.to_orders.to_customer '\
                'WHERE to_customer.c_mktsegment = ? AND to_orders.o_orderdate = ?'
            Q 'SELECT lineitem.l_orderkey, lineitem.l_extendedprice, to_orders.o_shippriority '\
                'FROM lineitem.to_orders.to_customer '\
                'WHERE to_customer.c_mktsegment = ? AND to_orders.o_shippriority = ?'
          end
        end

        indexes = PrunedIndexEnumerator.new(tpch_workload, cost_model,
                                            1, 100, 1).indexes_for_workload
        planner = QueryPlanner.new tpch_workload.model, indexes, cost_model
        tpch_workload.statement_weights.select{|s| s.instance_of? Query}.keys.each do |q|
          join_plans = planner.find_plans_for_query(q).select do |plan|
            index_lookup_steps = plan.steps.select{|s| s.is_a? Plans::IndexLookupPlanStep}
            index_lookup_steps.size > 1
          end
          expect(join_plans.size).to be > 0
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
            Q 'SELECT to_nation.n_name, lineitem.l_extendedprice, lineitem.l_discount ' \
              'FROM lineitem.to_orders.to_customer.to_nation.to_region ' \
              'WHERE to_region.r_name = ? AND to_orders.o_orderdate >= ? AND to_orders.o_orderdate < ? ' \
              'ORDER BY lineitem.l_extendedprice, lineitem.l_discount -- Q5'
            Q 'SELECT to_nation.n_name, lineitem.l_shipdate, '\
              'lineitem.l_extendedprice, lineitem.l_discount ' \
              'FROM lineitem.to_orders.to_customer.to_nation '\
              'WHERE lineitem.l_orderkey = ? AND lineitem.l_shipdate < ? AND lineitem.l_shipdate > ? ' \
              'ORDER BY to_nation.n_name, lineitem.l_shipdate -- Q7'
             Q 'SELECT to_orders.o_orderdate, from_lineitem.l_extendedprice, from_lineitem.l_discount, to_nation.n_name '\
               'FROM part.from_partsupp.from_lineitem.to_orders.to_customer.to_nation.to_region ' \
               'WHERE to_region.r_name = ? AND to_orders.o_orderdate < ? AND to_orders.o_orderdate > ? AND part.p_type = ? ' \
               'ORDER BY to_orders.o_orderdate -- Q8'
             Q 'SELECT to_nation.n_name, to_orders.o_orderdate, from_lineitem.l_extendedprice, from_lineitem.l_discount, '  \
               'from_partsupp.ps_supplycost, from_lineitem.l_quantity ' \
               'FROM part.from_partsupp.from_lineitem.to_orders.to_customer.to_nation ' \
               'WHERE part.p_name = ? AND from_lineitem.l_orderkey = ? ' \
               'ORDER BY to_nation.n_name, to_orders.o_orderdate -- Q9'
          end
        end
        indexes = PrunedIndexEnumerator.new(tpch_workload, cost_model, 1, 3, 1).indexes_for_workload.to_a
        pruned_planner = QueryPlanner.new tpch_workload.model, indexes, cost_model
        query_indexes_hash = tpch_workload.statement_weights.keys.flat_map do |q|
          pruned_plan = pruned_planner.find_plans_for_query q
          plan_indexes = pruned_plan.map{|p| p.select{|s| s.is_a? Plans::IndexLookupPlanStep}.map{|s| s.index}}.flatten
          Hash[q, plan_indexes]
        end.inject(&:merge)

        used_indexes = query_indexes_hash.values.map(&:uniq).flatten
        expect(used_indexes.size - used_indexes.uniq.size).to be 175
      end

      it 'enumerates only allowed depth query plan' do
        tpch_workload = Workload.new do |_|
          Model 'tpch'
          Group 'Group1', default: 1 do
            Q 'INSERT INTO orders SET o_orderkey=?, o_orderstatus=?, o_totalprice=?, o_orderdate=?, o_orderpriority=?, '\
                  'o_clerk=?, o_shippriority=?, o_comment=? AND CONNECT TO to_customer(?) -- 4'
            Q 'SELECT to_orders.o_orderdate, from_lineitem.l_extendedprice '\
              'FROM part.from_partsupp.from_lineitem.to_orders.to_customer.to_nation.to_region ' \
              'WHERE to_region.r_name = ? AND part.p_type = ?'
            Q 'SELECT to_orders.o_orderdate, from_lineitem.l_extendedprice '\
              'FROM part.from_partsupp.from_lineitem.to_orders.to_customer.to_nation.to_region ' \
              'WHERE to_region.r_name = ? AND part.p_brand = ?'
          end
        end
        1.upto(3).each do |index_step_size_threshold|
          indexes = PrunedIndexEnumerator.new(tpch_workload, cost_model,
                                              1, index_step_size_threshold, 1)
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

    describe PreparingQueryPlanner do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'only has IndexLookupSteps' do
        tpch_workload = TimeDependWorkload.new do |_|
          TimeSteps 3
          Model 'tpch'
          Group 'Group1', default: [1,2,3] do
            Q 'SELECT to_orders.o_orderpriority, count(to_orders.o_orderkey) ' \
              'FROM lineitem.to_orders '\
              'WHERE to_orders.o_orderkey = ? AND to_orders.o_orderpriority = ? AND lineitem.l_orderkey = ? ' \
              'GROUP BY to_orders.o_orderpriority -- Q4'
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
              expect(step.class).to be IndexLookupPlanStep
            end
          end
        end
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
    end
  end
end
