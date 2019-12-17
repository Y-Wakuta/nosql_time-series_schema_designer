# frozen_string_literal: true

module NoSE
  module Search
    # A container for results from a schema search
    class TimeDependResults < Results
      attr_accessor :timesteps, :migrate_plans, :time_depend_plans, :time_depend_indexes, :time_depend_update_plans, :each_total_cost

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

      # calculate cost in each timestep for output
      # @return [void]
      def calculate_cost_each_timestep
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
        plan_all_times = (0...@problem.timesteps).map do |ts|
          tree.find do |tree_plan|
            tree_plan.indexes.to_set == @query_indexes[query][ts]
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
                          .map(&:indexes)
                          .flatten(1)
        indexes_support_query = @time_depend_update_plans
                                  .map{|tdup| tdup.plans_all_timestep[timestep]
                                                .plans
                                                .map(&:query_plans)}
                                  .flatten(2)
                                  .map(&:indexes)
                                  .flatten(1)
        indexes_query + indexes_support_query
      end

      # given timestep is one of obsolete timestep
      def get_prepare_plans(plan, migrate_prepare_plans ,timestep)
        prepare_plans = []
        plan.each do |step|
          next unless step.is_a?(Plans::IndexLookupPlanStep)

          indexes_for_the_timestep = indexes_used_in_plans(timestep)

          # if the index already exists, we do not need to create the index
          next if indexes_for_the_timestep.include? step.index

          possible_plans = migrate_prepare_plans[step.index].select do |query_plan|
            query_plan.select {|s| s.is_a?(Plans::IndexLookupPlanStep)} \
                          .all? {|s| indexes_for_the_timestep.include? s.index }
          end

          # if min_plan has the same index as the target index, this prepare plan is unnecessary
          min_plan = possible_plans.sort_by {|qp| qp.cost}.first
          next if min_plan.indexes.length == 1 and min_plan.indexes.first == step.index

          prepare_plans << Plans::MigratePreparePlan.new(step.index, min_plan)
        end
        prepare_plans
      end

      def set_migrate_preparing_plans(migrate_prepare_plans)
        @time_depend_plans.each do |tdp|
          tdp.plans.each_cons(2).to_a.each_with_index do |(obsolete_plan, new_plan), ind|
            next if obsolete_plan == new_plan
            migrate_plan = Plans::MigratePlan.new(tdp.query, ind,  obsolete_plan, new_plan)
            migrate_plan.prepare_plans = get_prepare_plans(new_plan, migrate_prepare_plans, ind)
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
            plans.select do |plan|
              @indexes[ts].include? plan.index
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
      end

      # Ensure that all the query plans use valid indexes
      # @return [void]
      def validate_query_indexes(plans)
        plans.each do |plan|
          plan.each do |step|
            valid_plan = !step.is_a?(Plans::IndexLookupPlanStep) ||
              @indexes.reduce(Set.new){|b, n| b.merge(n)}.include?(step.index)
            fail InvalidResultsException unless valid_plan
          end
        end
      end

      # Ensure we only have necessary update plans which use available indexes
      # @return [void]
      def validate_update_indexes
        @update_plans.each do |_, plan_all_timestep|
          plan_all_timestep.each_with_index do |plans_each_time, ts|
            plans_each_time.each do |plan|
              validate_query_indexes plan.query_plans
              valid_plan = @indexes[ts].include?(plan.index)
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
        update_cost = @update_plans.reduce 0 do |sum, plan|
          sum + @workload.statement_weights[plan.statement] * plan.cost
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
            plan.indexes.to_set == @query_indexes[plan.query][timestep]
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
