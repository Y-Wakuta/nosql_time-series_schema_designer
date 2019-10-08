# frozen_string_literal: true

module NoSE
  module Search
    # A container for results from a schema search
    class TimeDependResults < Results
      attr_accessor :timesteps, :migrate_plans, :each_total_cost

      def initialize(problem = nil, by_id_graph = false)
        @problem = problem
        @timesteps = problem.timesteps
        return if problem.nil?
        @by_id_graph = by_id_graph
        @migrate_plans = []

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
        (@update_plans || []).each do |plan|
          plan.update_steps.each { |s| s.calculate_cost new_cost_model }
          plan.query_plans.each do |query_plan|
            query_plan.each { |s| s.calculate_cost new_cost_model }
          end
        end

        # Recalculate the total
        query_cost = (@plans || []).sum_by do |plan_all_times|
          plan_all_times.each_with_index.map do |plan, ts|
            plan.cost * @workload.statement_weights[plan.query][ts]
          end.sum
        end
        update_cost = (@update_plans || []).sum_by do |plan|
          plan.cost * @workload.statement_weights[plan.statement]
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

        update_cost = (@update_plans || []).each_with_index.map do |plan_each_times, ts|
          plan_each_times.map do |update_plan|
            update_plan.cost * @workload.statement_weights[update_plan.statement][ts]
          end.sum
        end
        @each_total_cost = query_cost.zip(update_cost).map(&:sum)
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
        set_migrate_plan(plan_all_times)

        plan_all_times
      end

      # Select the relevant update plans
      # @return [void]
      def set_update_plans(update_plans)
        update_plans = (0...@timesteps).map do |ts|
          update_plans.values.flatten(1).select do |plan|
            @indexes[ts].include? plan.index
          end.map{|plan| plan.dup}
        end
        update_plans.each_with_index do |plans_each_timestep, ts|
          plans_each_timestep.each do |plan|
            plan.select_query_plans(timestep: ts, &self.method(:select_plan))
          end
        end

        # TODO: update_plans here need to be an array of timesteps
        @update_plans = update_plans

        calculate_cost_each_timestep
      end

      # get the query plans for all timesteps for the query as parameter
      # @param [Array]
      # @return [void]
      def set_migrate_plan(plans)
        query = plans.compact.first.query
        plans.each_cons(2).to_a.each_with_index do |(form, nex), ind|
          next if form == nex or form.nil? or nex.nil?
          @migrate_plans << MigratePlan.new(query, ind, ind + 1, form, nex)
        end
      end

      class MigratePlan
        attr_reader :query, :start_time, :end_time, :obsolete_plan, :new_plan
        def initialize(query, start_time, end_time, obsolete_plan, new_plan)
          @query = query
          @start_time = start_time
          @end_time = end_time
          @obsolete_plan = obsolete_plan
          @new_plan = new_plan
        end
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

      # Ensure we only have necessary update plans which use available indexes
      # @return [void]
      def validate_update_indexes
        @update_plans.each_with_index do |plans_each_time, ts|
          plans_each_time.each do |plan|
            validate_query_indexes plan.query_plans
            valid_plan = @indexes[ts].include?(plan.index)
            fail InvalidResultsException unless valid_plan
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
      def validate_query_plans(plans)
        # Check that these indexes are actually used by the query
        plans.each do |plan|
          plan.each_with_index do |plan_one_step, ts|
            next if plan.compact.first.query.is_a? (SupportQuery) and plan_one_step.nil?

            fail InvalidResultsException unless \
            plan_one_step.indexes.to_set == @query_indexes[plan_one_step.query][ts]
          end
        end
      end

      # Validate the support query plans for each update
      # @return [void]
      def validate_update_plans
        @update_plans.each do |plans_each_time|
          plans_each_time.each do |plan|
            plan.instance_variable_set :@workload, @workload

            validate_query_plans plan.query_plans
          end
        end
      end
    end
  end
end
