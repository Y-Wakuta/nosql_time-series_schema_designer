# frozen_string_literal: true

require_relative 'search/constraints'
require_relative 'search/time_depend_constraints'
require_relative 'search/problem'
require_relative 'search/time_depend_problem'
require_relative 'search/results'
require_relative 'search/ideal_results'
require_relative 'search/time_depend_results'
require_relative 'plans/time_depend_plan'

require 'logging'
require 'ostruct'
require 'tempfile'
require 'parallel'
require 'etc'

module NoSE
  # ILP construction and schema search
  module Search
    # Searches for the optimal indices for a given workload
    class IdealSearch < Search

      # Search for optimal indices using an ILP which searches for
      # non-overlapping indices
      # @return [Results]
      def search_overlap(indexes, max_space = Float::INFINITY)
        return if indexes.empty?
        STDERR.puts("set basic query plans")

        # Get the costs of all queries and updates
        query_weights = combine_query_weights indexes
        RunningTimeLogger.info(RunningTimeLogger::Headers::START_QUERY_PLAN_ENUMERATION)
        costs, trees = query_costs query_weights, indexes
        RunningTimeLogger.info(RunningTimeLogger::Headers::END_QUERY_PLAN_ENUMERATION)

        show_tree trees

        # prepare_update_costs possibly takes nil value if the workload is not time-depend workload
        update_costs, update_plans, prepare_update_costs = update_costs trees, indexes

        log_search_start costs, query_weights

        solver_params = {
            max_space: max_space,
            costs: costs,
            update_costs: update_costs,
            prepare_update_costs: prepare_update_costs,
            cost_model: @cost_model,
            by_id_graph: @by_id_graph,
            trees: trees
        }

        fail "ideal search is for TimeDependWorkload" unless @workload.is_a? TimeDependWorkload

        search_result query_weights, indexes, solver_params, trees,
                      update_plans
      end

      private

      # Run the solver and get the results of search
      # @return [Results]
      def search_result(query_weights, indexes, solver_params, trees,
                        update_plans)

        workloads_each_ts = divide_workload_each_ts
        solver_params_each_ts = divide_solver_params_each_ts solver_params
        # Solve the LP using MIPPeR
        STDERR.puts "start optimization : #{Time.now}"
        RunningTimeLogger.info(RunningTimeLogger::Headers::START_WHOLE_OPTIMIZATION)
        results_each_ts = workloads_each_ts.zip(solver_params_each_ts).map do |workload_ts, solver_params_ts|
          solve_mipper workload_ts, query_weights.keys, indexes, update_plans, **solver_params_ts
        end
        RunningTimeLogger.info(RunningTimeLogger::Headers::END_WHOLE_OPTIMIZATION)

        result = IdealResults.new(results_each_ts)
        setup_result result, solver_params, update_plans
        result
      end

      # Solve the index selection problem using MIPPeR
      # @return [Results]
      def solve_mipper(workload, queries, indexes, update_plans, data)
        # Construct and solve the ILP
        problem = Problem.new(queries, workload.updates, data, @objective)

        problem.solve

        # We won't get here if there's no valdi solution
        @logger.debug 'Found solution with total cost ' \
                      "#{problem.objective_value}"

        # Return the selected indices
        selected_indexes = problem.selected_indexes

        @logger.debug do
          "Selected indexes:\n" + selected_indexes.map do |index|
            "#{indexes.index index} #{index.inspect}"
          end.join("\n")
        end

        problem.result
      end

      def divide_workload_each_ts
        static_workloads = (0...@workload.timesteps).map{|_| Workload.new(@workload.model)}
        @workload.statement_weights.each do |q, freqs|
          freqs.each_with_index {|freq, idx| static_workloads[idx].add_statement(q, freq)}
        end
        static_workloads
      end

      def divide_solver_params_each_ts(solver_params)
        fail "dividing solver params with update_costs are currently not supported" unless solver_params[:update_costs].empty?

        solver_params_each_ts = (0...@workload.timesteps).map do |_|
          {
            :max_space => solver_params[:max_space],
            :cost_model => solver_params[:cost_model],
            :trees => solver_params[:trees],
            :by_id_graph => solver_params[:by_id_graph],
            :update_costs => {}, # not supported. but this is easy to implement
            :costs => {}
          }
        end

        solver_params[:costs].each do |q, index_plans|
          index_plans.each do |index, (steps, costs)|
            costs.each_with_index do |cost, idx|
              solver_params_each_ts[idx][:costs][q] = {} if  solver_params_each_ts[idx][:costs][q].nil?
              solver_params_each_ts[idx][:costs][q][index] = [steps, cost]
            end
          end
        end
        solver_params_each_ts
      end
    end
  end
end
