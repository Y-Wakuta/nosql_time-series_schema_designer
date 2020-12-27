# frozen_string_literal: true

require_relative 'search/constraints'
require_relative 'search/time_depend_constraints'
require_relative 'search/time_depend_iterative_constraints'
require_relative 'search/problem'
require_relative 'search/time_depend_problem'
require_relative 'search/time_depend_iterative_problem'
require_relative 'search/results'
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
    class IterativeSearch < Search

      # Solve the index selection problem using MIPPeR
      # @return [Results]
      def solve_mipper(queries, indexes, update_plans, data)
        @update_plans = update_plans
        @workload_timesteps = @workload.timesteps - 1
        @base_workload = @workload

        @workload = get_subset_workload @workload, 0, @workload_timesteps
        reduced_data = refresh_solver_params indexes, @workload, data
        ts_indexes = solve_subset queries, indexes, reduced_data,
                                        @workload, 0, @workload_timesteps, {}

        @workload = @base_workload
        # Construct and solve the ILP
        problem = TimeDependIterativeProblem.new(queries, @base_workload, data, ts_indexes, @objective)
        problem.add_whole_step_constraints

        STDERR.puts "execute whole optimization"
        problem.solve

        problem.result
      end

      private

      def solve_subset(queries, indexes, data, workload, start_ts, end_ts, ts_indexes)
        middle_ts = ((end_ts - start_ts) / 2).ceil + start_ts
        STDERR.puts "-=-=start : " + start_ts.to_s + " middle: " + middle_ts.to_s + " end: "  + end_ts.to_s

        problem = TimeDependIterativeProblem.new(queries, workload, data,
                                                 ts_indexes, @objective)
        problem.start_ts = start_ts
        problem.middle_ts = middle_ts
        problem.end_ts = end_ts
        problem.add_iterative_constraints unless ts_indexes.empty?

        problem.solve
        setup_fixed_hash problem.result, ts_indexes, [start_ts, middle_ts, end_ts]

        left_workload = get_subset_workload workload, start_ts, middle_ts
        right_workload = get_subset_workload workload, middle_ts, end_ts
        ranges = []
        ranges << [left_workload, start_ts, middle_ts] unless left_workload.nil?
        ranges << [right_workload, middle_ts, end_ts] unless right_workload.nil?
        return ts_indexes if ranges.empty?

        whole_ts_index_hashes = Parallel.map(ranges, in_processes: 2) do |workload, left, right|
          data = refresh_solver_params indexes, workload, data
          solve_subset(queries, indexes, data, workload, left, right, ts_indexes)
        end.flatten
        merge_ts_indexes_hashes whole_ts_index_hashes
      end

      def merge_ts_indexes_hashes(ts_indexes_hashs)
        merged = ts_indexes_hashs.inject do |h1, h2|
          h1.merge(h2) do |_, oldval, newval|
            unless oldval == newval
              puts "ts_index value is not fixed"
              puts "oldval" + oldval.map{|v| v.key}.sort().inspect
              puts "newval" + newval.map{|v| v.key}.sort().inspect
            end
            oldval + newval
          end
        end
        merged
      end

      # Combine the weights of queries and statements
      # @return [void]
      def refresh_query_weights(support_queries, workload)
        query_weights = Hash[support_queries.map do |query|
          [query, workload.statement_weights[query.statement]]
        end]
        query_weights.merge!(workload.statement_weights.select do |stmt, _|
          stmt.is_a? Query
        end.to_h)

        query_weights
      end

      def refresh_query_costs(query_weights, trees)
        results = Parallel.map(trees, in_processes: Etc.nprocessors - 4) do |tree|
          refresh_query_cost tree, tree.query, query_weights[tree.query]
        end
        costs = Hash[query_weights.each_key.map.with_index do |query, q|
          [query, results[q].first]
        end]

        [costs, results.map(&:last)]
      end

      # Get the cost for indices for an individual query
      def refresh_query_cost(tree, query, weight)
        query_costs = {}

        tree.each do |plan|
          steps_by_index = []
          plan.each do |step|
            if step.is_a? Plans::IndexLookupPlanStep
              steps_by_index.push [step]
            else
              steps_by_index.last.push step
            end
          end
          populate_query_costs query_costs, steps_by_index, weight, query, tree
        end

        [query_costs, tree]
      end


      def refresh_solver_params(indexes, workload, solver_params)
        solver_params = solver_params.dup
        query_weights = refresh_query_weights solver_params[:costs].keys.select{|q| q.instance_of? SupportQuery}, workload

        costs, trees = refresh_query_costs query_weights, solver_params[:trees]

        update_costs, update_plans, prepare_update_costs = update_costs trees, indexes

        log_search_start costs, query_weights

        solver_params[:costs] = costs
        solver_params[:update_costs] = update_costs
        solver_params[:prepare_update_costs] = prepare_update_costs

        if @workload.is_a? TimeDependWorkload and not solver_params[:migrate_prepare_plans].empty?
          costs.merge!(solver_params[:migrate_prepare_plans].values
                           .flat_map{|v| v.values}
                           .map{|v| v[:costs]}
                           .reduce(&:merge))
        end
        solver_params
      end

      def setup_fixed_hash(result, ts_indexes, tss)
        result.indexes.zip(tss) do |indexes_ts|
          ts = indexes_ts.last
          ts_indexes[ts] = Set.new if ts_indexes[ts].nil?
          indexes_ts.first.each {|idx| ts_indexes[ts].add(idx)}
        end
      end

      def get_subset_workload(workload, start_ts, end_ts)
        middle_ts = ((end_ts - start_ts)/ 2).ceil + start_ts
        return if start_ts == middle_ts or middle_ts == end_ts
        sub_workload = TimeDependWorkload.new(model = workload.model) do
          Interval workload.interval * ((middle_ts - start_ts) + (end_ts - middle_ts)) / 2
          TimeSteps 3
        end

        workload.statement_weights.each do |statement, _|
          freq = @base_workload.statement_weights[statement]
                     .select.with_index { |_, idx| [start_ts, middle_ts, end_ts].include? idx}
                     .map{|f, _| f / @base_workload.interval}
          sub_workload.add_statement(statement, frequency: freq)
        end
        sub_workload
      end
    end
  end
end
