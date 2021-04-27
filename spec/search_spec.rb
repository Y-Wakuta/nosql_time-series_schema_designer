require_relative './support/dummy_cost_model'
require_relative './support/entities'

module NoSE
  module Search
    describe Search do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'raises an exception if there is no space', solver: true do
        workload.add_statement 'SELECT Tweet.Body FROM Tweet ' \
                               'WHERE Tweet.TweetId = ?'
        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        search = Search.new(workload, cost_model)
        expect do
          search.search_overlap(indexes, 1)
        end.to raise_error(NoSolutionException)
      end

      it 'produces a materialized view with sufficient space', solver: true do
        query = Statement.parse 'SELECT User.UserId FROM User WHERE ' \
                                'User.City = ? ORDER BY User.Username',
                                workload.model
        workload.add_statement query

        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        result = Search.new(workload, cost_model).search_overlap indexes
        indexes = result.indexes
        expect(indexes).to include query.materialize_view
      end

      it 'can perform multiple lookups on a path segment', solver: true do
        query = Statement.parse 'SELECT User.Username FROM User ' \
                                'WHERE User.City = ?', workload.model
        workload.add_statement query

        indexes = [
            Index.new([user['City']], [user['UserId']], [],
                      QueryGraph::Graph.from_path([user.id_field])),
            Index.new([user['UserId']], [], [user['Username']],
                      QueryGraph::Graph.from_path([user.id_field]))
        ]
        search = Search.new(workload, cost_model)
        expect do
          search.search_overlap(indexes, indexes.first.size).to_set
        end.to raise_error NoSolutionException
      end

      it 'does not denormalize heavily updated data', solver: true do
        workload.add_statement 'UPDATE User SET Username = ? ' \
                               'WHERE User.UserId = ?', 0.98
        workload.add_statement 'SELECT User.Username FROM User ' \
                               'WHERE User.City = ?', 0.01
        workload.add_statement 'SELECT User.Username FROM User ' \
                               'WHERE User.Country = ?', 0.01

        # Enumerate the indexes and select those actually used
        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        cost_model = Cost::EntityCountCost.new
        result = Search.new(workload, cost_model).search_overlap indexes
        indexes = result.indexes

        # Get the indexes actually used by the generated plans
        planner = Plans::QueryPlanner.new workload, indexes, cost_model
        plans = workload.queries.map { |query| planner.min_plan query }
        indexes = plans.flat_map(&:indexes).to_set

        expect(indexes).to match_array [
                                           Index.new([user['Country']], [user['UserId']], [],
                                                     QueryGraph::Graph.from_path([user.id_field])),
                                           Index.new([user['City']], [user['UserId']], [],
                                                     QueryGraph::Graph.from_path([user.id_field])),
                                           Index.new([user['UserId']], [], [user['Username']],
                                                     QueryGraph::Graph.from_path([user.id_field]))
                                       ]
      end

      it 'increases the total cost when an update is added' do
        query = Statement.parse 'SELECT User.UserId FROM User WHERE ' \
                                'User.City = ? ORDER BY User.Username', workload.model

        workload.add_statement query
        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        result = Search.new(workload, cost_model).search_overlap indexes

        workload.add_statement 'UPDATE User SET Username = ? ' \
                               'WHERE User.UserId = ?', 0.98

        indexes_with_update = IndexEnumerator.new(workload).indexes_for_workload.to_a
        result_with_update = Search.new(workload, cost_model).search_overlap indexes_with_update

        # total cost should be increased due to additional update statement
        expect(result.total_cost).to be < result_with_update.total_cost
      end

      it 'is able to deal with multiple equal predicate on one entity' do
        workload.add_statement(Statement.parse 'SELECT User.* FROM User ' \
                                                     'WHERE User.UserId = ? AND User.City = ?', workload.model)

        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        expect do
          Search.new(workload, cost_model).search_overlap indexes
        end.not_to raise_error
      end

      it 'provide solution even when the query include aggregation fields' do
        workload.add_statement(Statement.parse 'SELECT count(Tweet.TweetId), sum(Tweet.Retweets), Tweet.Timestamp FROM Tweet WHERE ' \
                                'Tweet.Body = ?', workload.model)
        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        result = Search.new(workload, cost_model).search_overlap indexes
        expect(result.plans).to have(1).plan
      end

      it 'provide query plan that does aggregation on database if there was no other queries' do
        workload.add_statement(Statement.parse 'SELECT count(Tweet.TweetId), Tweet.Retweets, count(Tweet.Timestamp) FROM Tweet WHERE ' \
                                'Tweet.Body = ? GROUP BY Tweet.Retweets', workload.model)
        indexes = IndexEnumerator.new(workload).indexes_for_workload.to_a
        result = Search.new(workload, cost_model).search_overlap indexes
        expect(result.plans).to have(1).plan
        expect(result.plans.first.steps).to have(1).step
        expect(result.plans.first.steps.first.index.count_fields).to eq(Set.new([tweet['TweetId'], tweet['Timestamp']]))
        expect(result.plans.first.steps.first.index.groupby_fields).to eq(Set.new([tweet['Retweets']]))
      end

      let(:cost_model) do
        class TmpDummyCost < NoSE::Cost::Cost
          include Subtype
          def index_lookup_cost(_step)
            return nil if _step.state.nil?
            rows = _step.state.cardinality
            parts = _step.state.hash_cardinality
            0.0078395645 + parts * 0.0013692786 +
                rows * 1.17093638386496e-005
          end

          def delete_cost(step)
            0.001
          end

          def insert_cost(step)
            0.001
          end
        end
        TmpDummyCost.new
      end

      it 'choices materialized view plan for only one query' do
        tpch_workload = Workload.new do |_|
          Model 'tpch'
          Group 'Group1', default: 1 do
            Q 'SELECT c_nationkey.n_name, sum(lineitem.l_extendedprice), sum(lineitem.l_discount) ' \
              'FROM lineitem.l_orderkey.o_custkey.c_nationkey.n_regionkey ' \
              'WHERE n_regionkey.r_name = ? AND l_orderkey.o_orderdate >= ? AND l_orderkey.o_orderdate < ? ' \
              'ORDER BY lineitem.l_extendedprice, lineitem.l_discount ' \
              'GROUP BY c_nationkey.n_name -- Q5'
          end
        end
        indexes = GraphBasedIndexEnumerator.new(tpch_workload, cost_model, 2, 1_000)
                      .indexes_for_workload.to_a
        result = Search.new(tpch_workload, cost_model).search_overlap indexes
        plan = result.plans.first
        mv = plan.query.materialize_view_with_aggregation
        expect(plan.indexes.first).to eq mv
      end
    end
  end
end
