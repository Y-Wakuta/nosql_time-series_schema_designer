require_relative './support/dummy_cost_model'
require_relative './support/entities'

module NoSE
  module Search
    describe IterativeSearch do
      include_context 'dummy cost model'
      include_context 'entities'

      it 'produces same migration plans as one-time optimization' do
        ts = 5
        tpch_workload = NoSE::TimeDependWorkload.new do
          TimeSteps ts
          Interval 200
          Model 'rubis'

          Group 'Test1', 1.0, default: (0...ts).map{|i| 10 ** i} do
            Q 'SELECT users.* FROM users WHERE users.id = ? -- 0'
            Q 'SELECT users.* FROM users WHERE users.rating=? -- 1'
            Q 'INSERT INTO users SET id = ?, rating = ? -- 2'
          end

          Group 'Test2', 1.0, default: (0...ts).map{|i| 10 ** i}.reverse do
            Q 'SELECT items.* FROM items WHERE items.id = ? -- 3'
            Q 'SELECT items.* FROM items WHERE items.quantity=? -- 4'
            Q 'INSERT INTO items SET id = ?, quantity = ? -- 5'
          end
        end

        indexes = IndexEnumerator.new(tpch_workload).indexes_for_workload.to_a
        result = Search.new(tpch_workload, cost_model).search_overlap indexes, 12250000
        expect(result.migrate_plans.size).to eq 2
        iterative_result = IterativeSearch.new(tpch_workload, cost_model).search_overlap indexes, 12250000
        expect(iterative_result.migrate_plans.size).to eq 2
      end
    end
  end
end
