require_relative './support/dummy_cost_model'

module NoSE
  module Search
    describe Search do
      include_context 'dummy cost model'
      let(:timesteps) { 3 }
      let(:query_increase) {'SELECT users.* FROM users WHERE users.rating=? -- 1'}
      let(:query_decrease) {'SELECT items.* FROM items WHERE items.quantity=? -- 3'}

      let(:td_workload) {
        ts = timesteps
        qi = query_increase
        qd = query_decrease
        TimeDependWorkload.new do
          TimeSteps ts
          DefaultMix :default
          Interval 3600
          Model 'rubis'

          Group 'Test1', 1.0, default: [0.001, 0.5, 9] do
            Q 'SELECT users.* FROM users WHERE users.id = ? -- 0'
            Q qi
          end

          Group 'Test2', 1.0, default: [9, 0.5, 0.001] do
            Q 'SELECT items.* FROM items WHERE items.id = ? -- 2'
            Q qd
          end
        end
      }

      it 'updates migration-preparing index during executing migration' do
        update_user = 'UPDATE users SET rating=?, firstname = ?, lastname = ? WHERE users.id=? -- 27'
        td_workload.add_statement update_user, frequency: [0.001, 0.5, 9]
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes, 12250000
        update_user_plans = result.time_depend_update_plans.first.plans_all_timestep
        target_timestep = 0
        updated_indexes = update_user_plans[target_timestep].plans.map(&:index).to_set
        current_indexes_used_by_users = result.time_depend_plans
                                          .select{|tdps| tdps.query.entity.name == 'users'}
                                          .map{|p| p.plans.fetch(target_timestep)
                                                     .map(&:index)
                                          }
                                          .flatten(1)
                                          .to_set

        next_indexes_used_by_users = result.time_depend_plans
                                          .select{|tdps| tdps.query.entity.name == 'users'}
                                          .map{|p| p.plans.fetch(target_timestep + 1)
                                                     .map(&:index)
                                          }
                                          .flatten(1)
                                          .to_set

        new_indexes = next_indexes_used_by_users - current_indexes_used_by_users
        expect(updated_indexes).to include(new_indexes)
      end

      it 'gives migration plan if we ignore migration cost even when given high migration cost' do
        td_workload.creation_coeff = 1000
        td_workload.migrate_support_coeff = 1000

        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes, 12250000
        expect(result.migrate_plans.size).to be(0)

        td_workload.include_migration_cost = false
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes, 12250000
        expect(result.migrate_plans.size).to be(2)
      end

      it 'correct number of timesteps in output' do
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes
        expect(result.indexes.size).to eq timesteps # check for indexes
        expect(result.plans.map{|plan| plan.size}.uniq.first).to eq timesteps # check for plans
      end

      it 'the query plan changes when the frequency changes' do
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes, 12250000

        increase_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_increase}.flatten(1)
        decrease_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_decrease}.flatten(1)

        expect(increase_steps.first.steps.size).to be > increase_steps.last.steps.size
        expect(decrease_steps.first.steps.size).to be < decrease_steps.last.steps.size
      end

      it 'the query plan does not change when the creation cost is too large' do
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        td_workload.creation_coeff = 1000
        result = Search.new(td_workload, cost_model).search_overlap indexes, 12250000

        increase_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_increase}.flatten(1)
        decrease_steps = result.plans.select{|plan_all| plan_all.first.query.text == query_decrease}.flatten(1)

        expect(increase_steps.first.steps.size).to eq increase_steps.last.steps.size
        expect(decrease_steps.first.steps.size).to eq decrease_steps.last.steps.size
      end

      it 'is able to treat with update' do
        update = 'UPDATE users SET rating=? WHERE users.id=? -- 8'
        td_workload.add_statement update, frequency: [9, 0.5, 0.01]
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes

        expect(result.update_plans.values.first.size).to eq timesteps
      end

      it 'migrates plan is set when there is migration' do
        indexes = IndexEnumerator.new(td_workload).indexes_for_workload.to_a
        result = Search.new(td_workload, cost_model).search_overlap indexes, 12250000

        expect(result.migrate_plans.size).to eq 2
      end

      it 'migrates plan is not set if there is no interval' do
        ts = timesteps
        qi = query_increase
        qd = query_decrease
        td_workload_no_interval = TimeDependWorkload.new do
          TimeSteps ts
          DefaultMix :default
          Interval 0
          Model 'rubis'

          Group 'Test1', 1.0, default: [0.001, 0.5, 9] do
            Q 'SELECT users.* FROM users WHERE users.id = ? -- 0'
            Q qi
          end

          Group 'Test2', 1.0, default: [9, 0.5, 0.001] do
            Q 'SELECT items.* FROM items WHERE items.id = ? -- 2'
            Q qd
          end
        end

        indexes = IndexEnumerator.new(td_workload_no_interval).indexes_for_workload.to_a
        result = Search.new(td_workload_no_interval, cost_model).search_overlap indexes, 12250000

        expect(result.migrate_plans.size).to eq 0
      end

      it 'time depend workload get the same cost as static workload if the frequency does not change' do
        interval = 3600
        timesteps = 3
        td_workload_ = TimeDependWorkload.new do
          TimeSteps timesteps
          Interval interval

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

          Group 'Test1', 0.5, default: [0.01] * timesteps do
            Q 'SELECT users.* FROM users WHERE users.id = ? -- 0'
            Q 'SELECT items.* FROM items WHERE items.id=? -- 2'
            Q 'UPDATE users SET rating=? WHERE users.id=? -- 27'
          end
        end

        workload_ = Workload.new do

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

          Group 'Test1', 0.5, default: 0.01 do
            Q 'SELECT users.* FROM users WHERE users.id = ? -- 0'
            Q 'SELECT items.* FROM items WHERE items.id=? -- 2'
            Q 'UPDATE users SET rating=? WHERE users.id=? -- 27'
          end
        end

        td_indexes = IndexEnumerator.new(td_workload_).indexes_for_workload.to_a
        td_result = Search.new(td_workload_, cost_model).search_overlap td_indexes
        indexes = IndexEnumerator.new(workload_).indexes_for_workload.to_a
        result = Search.new(workload_, cost_model).search_overlap indexes

        expect(td_result.total_cost).to be_within(0.0001).of(result.total_cost * interval * timesteps)
      end

      it 'migrate plan is not set when the workload is static' do
        ts = timesteps
        td_workload_static = TimeDependWorkload.new do
          TimeSteps ts
          Static true

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

          Group 'Test1', 0.5, default: [0.01, 0.5, 9] do
            Q 'SELECT users.* FROM users WHERE users.id = ? -- 0'
            Q 'SELECT items.* FROM items WHERE items.id=? -- 2'
          end
        end

        query_increase = 'SELECT users.* FROM users WHERE users.rating=? -- 1'
        query_decrease = 'SELECT items.* FROM items WHERE items.quantity=? -- 3'
        td_workload_static.add_statement query_increase, frequency: [0.01, 0.5, 9]
        td_workload_static.add_statement query_decrease, frequency: [9, 0.5, 0.01]
        indexes = IndexEnumerator.new(td_workload_static).indexes_for_workload.to_a
        result = Search.new(td_workload_static, cost_model).search_overlap indexes, 9800000

        expect(result.migrate_plans.size).to eq 0
      end

      it 'migration preparing plan produced in the result' do
        rubis_workload = NoSE::TimeDependWorkload.new do
          Model 'rubis'
          increase = [1,2,3]
          TimeSteps increase.size
          Group 'ViewBidHistory', default: increase do
            Q 'SELECT bids.* FROM items.bids WHERE items.id = ? -- 6'
            Q 'SELECT bids.qty, bids.date FROM bids.item WHERE item.id=? ' \
              'ORDER BY bids.bid LIMIT 2 -- 19'
            Q 'INSERT INTO items SET id=?, name=?, description=?, initial_price=?, ' \
              'quantity=?, reserve_price=?, buy_now=?, nb_of_bids=0, max_bid=0, ' \
              'start_date=?, end_date=? AND CONNECT TO category(?), seller(?) -- 10'
          end
        end

        indexes = PrunedIndexEnumerator.new(rubis_workload, cost_model, 1, 2).indexes_for_workload.to_a
        #indexes = IndexEnumerator.new(rubis_workload).indexes_for_workload.to_a
        expect do
          Search.new(rubis_workload, cost_model).search_overlap indexes
        end.not_to raise_error
      end
    end
  end
end
