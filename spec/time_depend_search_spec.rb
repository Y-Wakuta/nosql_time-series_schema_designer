require_relative './support/dummy_cost_model'

module NoSE
  module Search
    describe Search do
      include_context 'dummy cost model'
      let(:timesteps) { 3 }

      let(:td_workload) {
        ts = timesteps
        TimeDependWorkload.new do
          TimeSteps ts

          (Entity 'users' do
            ID         'id'
            String     'firstname', 6
            String     'lastname', 7
            String     'rating', 23
          end) * 2_000

          (Entity 'items' do
            ID         'id'
            String     'name', 19
            String     'description', 197
            Integer    'quantity', count: 100
          end) * 20_000

          Group 'Test1', 0.5, :increase do
            Q 'SELECT users.* FROM users WHERE users.id = ? -- 0', [0.01, 0.5, 9]
            Q 'SELECT items.* FROM items WHERE items.id=? -- 2', [9, 0.5, 0.01]
          end
        end
      }

      it 'correct number of timesteps in output' do
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes
        expect(result.indexes.size).to eq timesteps # check for indexes
        expect(result.plans.map{|plan| plan.size}.uniq.first).to eq timesteps # check for plans
      end

      it 'the query plan changes when the frequency changes' do
        query_increase = 'SELECT users.* FROM users WHERE users.rating=? -- 1'
        query_decrease = 'SELECT items.* FROM items WHERE items.quantity=? -- 3'
        td_workload.add_statement query_increase, [0.01, 0.5, 9]
        td_workload.add_statement query_decrease, [9, 0.5, 0.01]
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes, 9800000

        increase_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_increase}.flatten(1)
        decrease_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_decrease}.flatten(1)

        expect(increase_steps.first.steps.size).to be >  increase_steps.last.steps.size
        expect(decrease_steps.first.steps.size).to be <  decrease_steps.last.steps.size
      end

      it 'the query plan does not change when the creation cost is too large' do
        query_increase = 'SELECT users.* FROM users WHERE users.rating=? -- 1'
        query_decrease = 'SELECT items.* FROM items WHERE items.quantity=? -- 3'
        td_workload.add_statement query_increase, [0.01, 0.5, 9]
        td_workload.add_statement query_decrease, [9, 0.5, 0.01]
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes, 9800000, 10

        increase_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_increase}.flatten(1)
        decrease_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_decrease}.flatten(1)

        expect(increase_steps.first.steps.size).to eq increase_steps.last.steps.size
        expect(decrease_steps.first.steps.size).to eq decrease_steps.last.steps.size
      end
    end
  end
end
