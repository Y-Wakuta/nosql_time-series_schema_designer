# frozen_string_literal: true

module NoSE
  module Search
    # A container for results from a schema search
    class TimeDependResults < Results
      attr_accessor :timesteps, :migrate_plans, :time_depend_plans, :time_depend_indexes, :time_depend_update_plans, :each_total_cost
      attr_reader :query_indexes

      def initialize(problem = nil, by_id_graph = false)
        @problem = problem
        return if problem.nil?
        @timesteps = problem.timesteps
        @by_id_graph = by_id_graph
        @migrate_plans = []
        @time_depend_plans = []
        @each_indexes = []

        # Find the indexes the ILP says the query should use
        @query_indexes = Hash.new
        @problem.query_vars.each do |index, query_var|
          query_var.each do |query, time_var|
            time_var.each do |ts, var|
              next unless var.value
              @query_indexes[query] = {} if @query_indexes[query].nil?
              @query_indexes[query][ts] = Set.new if @query_indexes[query][ts].nil?
              @query_indexes[query][ts].add index
            end
          end
        end

        # find indexes for MigrateSupportQuery
        @problem.prepare_vars.each do |index, query_var|
          query_var.each do |query, time_var|
            time_var.each do |ts, var|
              next unless var.value
              STDERR.puts "prepare_vars is 1 for #{index.key}, ms_q: #{query.index.key}, migrate_vars: #{@problem.migrate_vars[query.index][ts + 1].value}, #{ts} -> #{ts + 1}"

              # the index of migrate support query is newly created
              next unless problem.migrate_vars[query.index][ts + 1].value

              @query_indexes[query] = {} if @query_indexes[query].nil?
              @query_indexes[query][ts] = Set.new if @query_indexes[query][ts].nil?
              @query_indexes[query][ts].add index
            end
          end
        end

        @problem.migrate_vars.each do |index, m_vars|
          m_vars.each do |ts, var|
            next unless var.value
            STDERR.puts "migrate_vars, index : #{index.key} #{index.size}, ts: #{ts}"
          end
        end
      end

      # After setting the cost model, recalculate the cost
      # @return [void]
      def recalculate_cost(new_cost_model = nil)
        new_cost_model = @cost_model if new_cost_model.nil?

        (@plans || []).each do |plan_all_times|
          plan_all_times.each do |plan|
            plan.each { |s| s.calculate_cost new_cost_model }
          end
        end
        (@update_plans || {}).each do |_, plan_all_timesteps|
          plan_all_timesteps.each do |plan_each_timestep|
            plan_each_timestep.each {|plan| plan.update_steps.each { |s| s.calculate_cost new_cost_model }}
            plan_each_timestep.each do |plan|
              plan.query_plans.each do |query_plan|
                query_plan.each { |s| s.calculate_cost new_cost_model }
              end
            end
          end
        end

        # Recalculate the total
        query_cost = (@plans || []).sum_by do |plan_all_times|
          plan_all_times.each_with_index.map do |plan, ts|
            plan.cost * @workload.statement_weights[plan.query][ts]
          end.sum
        end
        update_cost = (@update_plans || {}).each.sum_by do |_, plan_all_timestep|
          plan_all_timestep.each_with_index.sum_by do |plan_each_timestep, ts|
            plan_each_timestep.sum_by do |plan|
              plan.cost * @workload.statement_weights[plan.statement][ts]
            end
          end
        end
        @total_cost = query_cost + update_cost
      end

      def calculate_workload_cost
         query_cost = (@plans || []).transpose.each_with_index.map do |plan_each_times, ts|
          plan_each_times.map do |plan|
            plan.cost * @workload.statement_weights[plan.query][ts]
          end.sum
        end

        update_cost = (@update_plans || {}).map do |_, plans_all_timestep|
          plans_all_timestep.each_with_index.map do |plans_each_timestep, ts|
            plans_each_timestep.map do |update_plan|
              update_plan.cost * @workload.statement_weights[update_plan.statement][ts]
            end.sum
          end
        end
        @each_total_cost = query_cost.zip(update_cost.transpose.map(&:sum)).map do |l, r|
          (l.nil? ? 0 : l) + (r.nil? ? 0 : r)
        end
      end

      # calculate cost in each timestep for output
      # @return [void]
      def calculate_cost_each_timestep
        calculate_workload_cost

        # TODO: this method only calculate the cost of extracting and loading cost of migration. Therefore, this should also calculate the cost of update of creating CFs for migration
        migrate_extract_cost = (0...@timesteps).map do |ts|
          @migrate_plans.select{|mp| mp.start_time == ts}.map(&:cost).inject(0, &:+)
        end
        puts "extracting cost for migration: #{migrate_extract_cost.inspect}"
        @each_total_cost = @each_total_cost.zip(migrate_extract_cost).map {|l, r| l + r}

        loading_cost = [0] * @timesteps
        @problem.migrate_vars.each do |index, vars_each_ts|
          vars_each_ts.each do |ts, var|
            next unless var.value
            loading_cost[ts] += cost_model.load_cost(index)
          end
        end
        puts "loading cost for migration: #{loading_cost.inspect}"
        @each_total_cost = @each_total_cost.zip(loading_cost).map {|l, r| l + r}

        @each_total_cost
      end

      # @return [void]
      def validate_query_set
        planned_queries = plans.flatten(1).map(&:query).to_set
        fail InvalidResultsException unless \
          (@workload.queries.to_set - planned_queries).empty?
      end

      # Select the single query plan from a tree of plans
      # @return [Plans::QueryPlan]
      # @raise [InvalidResultsException]
      def select_plan(tree)
        query = tree.query
        plan_all_times = (0...@timesteps).map do |ts|
          tree.find do |tree_plan|
            tree_plan.indexes.to_set == @query_indexes.select{|q, idxes| q == query}.values.first[ts]
          end
        end

        # support query possibly does not have plans for all timesteps
        fail InvalidResultsException if plan_all_times.any?{|plan| plan.nil?} and not query.is_a?(SupportQuery)

        plan_all_times.compact.each {|plan| plan.instance_variable_set :@workload, @workload}

        plan_all_times
      end

      def set_time_depend_plans
        @time_depend_plans = @plans.map do |plan|
          Plans::TimeDependPlan.new(plan.first.query, plan)
        end
      end

      def set_time_depend_indexes
        indexes_all_timestep = @indexes.map do |index|
          EachTimeStepIndexes.new(index)
        end
        @time_depend_indexes = TimeDependIndexes.new(indexes_all_timestep)
      end

      def set_time_depend_update_plans
        @time_depend_update_plans = @update_plans.map do |update, plans|
          Plans::TimeDependUpdatePlan.new(update, plans)
        end
      end

      def indexes_used_in_plans(timestep)
        indexes_query = @time_depend_plans
                            .map{|tdp| tdp.plans[timestep]}
                            .flat_map(&:indexes)
        indexes_support_query = @time_depend_update_plans
                                    .map{|tdup| tdup.plans_all_timestep[timestep]
                                                    .plans
                                                    .map(&:query_plans)}
                                    .flatten(2)
                                    .flat_map(&:indexes)
        (indexes_query + indexes_support_query).uniq
      end

      # given timestep is one of obsolete timestep
      def get_prepare_plan(new_index, migrate_prepare_plans, timestep)
        # if step.index already exists at timestep t, new prepare plan for timestep t + 1 is not required
        return nil if @indexes[timestep].include? new_index

        migrate_support_queries = migrate_prepare_plans.select{|idx| idx == new_index}
        migrate_support_query = migrate_support_queries.flat_map do |idx, support_query_tree|
          support_query_tree.keys.select do |support_query|
            @problem.prepare_tree_vars[idx][support_query][timestep].value
          end
        end.first

        return nil unless migrate_prepare_plans.has_key? new_index

        migrate_support_tree = migrate_prepare_plans[new_index][migrate_support_query][:tree]
        prepare_plan_for_the_timestep = migrate_support_tree.select do |plan|
          plan.indexes.to_set == @query_indexes[migrate_support_query]&.fetch(timestep, nil)
        end

        fail 'migrate prepare plan was not created' if prepare_plan_for_the_timestep.empty?
        fail "more than one query plan found for one query" if prepare_plan_for_the_timestep.size > 1
        fail "New CF cannot be included in the preparing plan" if prepare_plan_for_the_timestep.first
                                                                      .steps
                                                                      .select{|s| s.is_a? Plans::IndexLookupPlanStep}
                                                                      .map{|s| s.index}
                                                                      .include? new_index
        Plans::MigratePreparePlan.new(new_index, prepare_plan_for_the_timestep.first, timestep)
      end

      def get_prepare_plans(new_plan, migrate_prepare_plans, ts)
        new_plan.select{|s| s.is_a? Plans::IndexLookupPlanStep}.map do |new_step|
          get_prepare_plan(new_step.index, migrate_prepare_plans, ts)
        end.compact
      end

      def set_migrate_preparing_plans(migrate_prepare_plans)
        @time_depend_plans.each do |tdp|
          tdp.plans.each_cons(2).to_a.each_with_index do |(obsolete_plan, new_plan), ts|
            next if obsolete_plan == new_plan
            migrate_plan = Plans::MigratePlan.new(tdp.query, ts,  obsolete_plan, new_plan)
            migrate_plan.prepare_plans = get_prepare_plans(new_plan, migrate_prepare_plans, ts)
            @migrate_plans << migrate_plan
          end
        end

        @time_depend_update_plans.each do |tdup|
          tdup.plans_all_timestep.each_cons(2).to_a.each_with_index do |(obsolete_tdupet, new_tdupet), ind|
            obsolete_query_plans = obsolete_tdupet.plans.map{|p| p.query_plans}.compact.flatten(1)
            new_query_plans = new_tdupet.plans.map{|p| p.query_plans}.compact.flatten(1)

            (new_query_plans - obsolete_query_plans).each do |plan_2_create|
              migrate_plan = Plans::MigratePlan.new(tdup.statement, ind, nil, plan_2_create)
              migrate_plan.prepare_plans = get_prepare_plans(plan_2_create, migrate_prepare_plans, ind)
              next if migrate_plan.prepare_plans.empty?
              @migrate_plans << migrate_plan
            end
          end
        end
      end

      # Select the relevant update plans
      # @return [void]
      def set_update_plans(_update_plans)
        update_plans = {}
        _update_plans.map do |update, plans|
          update_plans[update] = (0...@timesteps).map do |ts|
            target_indexes = @indexes[ts]
            target_indexes += @indexes[ts + 1] if (ts + 1) < @timesteps
            plans.select do |plan|
              target_indexes.include? plan.index
            end.map{|plan| plan.dup}
          end
        end

        update_plans.each do |_, plans|
          plans.each_with_index do |plans_each_timestep, ts|
            plans_each_timestep.each do |plan|
              plan.select_query_plans(timestep: ts, &self.method(:select_plan))
              plan.query_plans = plan.query_plans.map {|p| p[ts]} # we only need query_plan for the timestep
            end
          end
        end

        # TODO: update_plans here need to be an array of timesteps
        @update_plans = update_plans
      end

      private

      # Check that the indexes selected were actually enumerated
      # @return [void]
      def validate_indexes
        # We may not have enumerated ID graphs
        check_indexes = @indexes.dup
        @indexes.each do |index|
          check_indexes.delete index.to_id_graph
        end if @by_id_graph

        check_indexes.each do |check_indexes_one_timestep|
          fail InvalidResultsException unless \
          (check_indexes_one_timestep - @enumerated_indexes).empty?
        end

        validate_migrate_plans
      end

      def validate_migrate_plans
        (0...(@timesteps - 1)).each do |now|
          # all new CF need to have preparing plans
          (@indexes[now + 1] - @indexes[now]).each do |new_index|
            #(indexes_used_in_plans(now + 1) - indexes_used_in_plans(now)).each do |new_index|
            prepare_plans = @migrate_plans.select{|mp| mp.start_time == now}.map{|mp| mp.prepare_plans}.flatten(1)

            fail "prepare plan not found for :" + new_index.inspect unless prepare_plans.any?{|pp| pp.index == new_index}
          end

          #fail 'now + 1 does not match' unless @indexes[now + 1].to_set == indexes_used_in_plans(now + 1).to_set
          #fail 'now does not match' unless @indexes[now].to_set == indexes_used_in_plans(now).to_set
        end
      end

      # @return [void]
      def validate_query_indexes(td_plans)
        td_plans.transpose.each_with_index do |plans_each_ts, ts|
          validate_plans_use_existing_indexes plans_each_ts, @indexes[ts]

          # Ensure that all of existing indexes are used in at least one query plan
          current_used_indexes = plans_each_ts.flat_map(&:indexes).to_set
          indexes_for_upseart = update_plans.flat_map{|_, upseart| upseart[ts]}
                                    .flat_map(&:query_plans)
                                    .flat_map(&:indexes)
                                    .uniq.to_set
          if @indexes[ts].to_set > (current_used_indexes + indexes_for_upseart)
            puts "== unused indexes: "
            (@indexes[ts].to_set - (current_used_indexes + indexes_for_upseart)).sort_by{|i| i.key}.each do |i|
              puts "#{ts} -- #{i.key} : #{i.hash_str}, index.size: #{i.size}"
            end
          end
        end
      end

      # Ensure we only have necessary update plans which use available indexes
      # @return [void]
      def validate_update_indexes
        @update_plans.each do |_, plan_all_timestep|
          plan_all_timestep.each_with_index do |plans_each_time, ts|
            plans_each_time.each do |plan|
              validate_plans_use_existing_indexes plan.query_plans, @indexes[ts]
              valid_plan = @indexes[ts].include?(plan.index)

              # allow updating the index in the next timestep if it is for the migration
              if @problem.migrate_vars[plan.index][ts + 1]&.value
                valid_plan_for_preparing = @indexes[ts + 1].include?(plan.index)
                valid_plan ||= valid_plan_for_preparing
              end

              fail InvalidResultsException unless valid_plan
            end
          end
        end
      end

      # @return [void]
      def validate_cost_objective
        query_cost = @plans.reduce 0 do |_, plan_all_timestep|
          plan_all_timestep.each_with_index.map do |plan_each_timestep, ts|
            @workload.statement_weights[plan_each_timestep.query][ts] * plan_each_timestep.cost
          end.inject(&:+)
        end
        update_cost = @update_plans.reduce 0 do |sum, (_, all_ts_plans)|
          all_ts_plans.each_with_index do |plans, ts|
            plans.each do |plan|
              sum += @workload.statement_weights[plan.statement][ts] * plan.cost
            end
          end
          sum
        end
        cost = query_cost + update_cost

        fail InvalidResultsException unless (cost - @total_cost).abs < 0.001
      end

      # @return [void]
      def validate_space_objective
        size = @indexes.map{|indexes_each_timestep| indexes_each_timestep.map(&:size).inject(&:+)}
        fail InvalidResultsException unless size == @total_size # TODO: total_size は時刻ごとの配列にする必要があると思う
      end

      # Validate the query plans from the original workload
      # @return [void]
      def validate_query_plans(plans_all_timestep)
        # Check that these indexes are actually used by the query
        plans_all_timestep.each do |plan_each_timestep|
          plan_each_timestep.each_with_index do |plan, ts|
            fail InvalidResultsException unless \
            plan.indexes.to_set == @query_indexes[plan.query][ts]
          end
        end
      end

      # Validate the query plans from the original workload
      # @return [void]
      def validate_support_query_plans(plans, timestep)
        plans.each do |plan|
          fail InvalidResultsException unless \
          plan.indexes.to_set == @query_indexes.select{|q, idxes| q == plan.query}.values.first[timestep]
        end
      end

      # Validate the support query plans for each update
      # @return [void]
      def validate_update_plans
        @update_plans.each do |_, plans_all_time|
          plans_all_time.each_with_index do |plans_each_time, timestep|
            plans_each_time.each do |plan|
              plan.instance_variable_set :@workload, @workload

              validate_support_query_plans plan.query_plans, timestep
            end
          end
        end
      end
    end
  end
end
