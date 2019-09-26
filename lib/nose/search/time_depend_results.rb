# frozen_string_literal: true

module NoSE
  module Search
    # A container for results from a schema search
    class TimeDependResults < Results
      attr_accessor :timesteps

      def initialize(problem = nil, by_id_graph = false)
        @problem = problem
        @timesteps = problem.timesteps
        return if problem.nil?
        @by_id_graph = by_id_graph

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
        plan_all_times.each {|plan| plan.instance_variable_set :@workload, @workload}

        fail InvalidResultsException if plan_all_times.any?{|plan| plan.nil?}
        plan_all_times
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
            fail InvalidResultsException unless \
            plan_one_step.indexes.to_set == @query_indexes[plan_one_step.query][ts]
          end
        end
      end
    end
  end
end
