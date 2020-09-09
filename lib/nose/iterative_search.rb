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
        @data = data[:trees]
        @update_plans = update_plans
        @workload_timesteps = @workload.timesteps - 1

        ts_indexes = {}
        ts_query_indexes = {}
        reduced_workload = get_subset_workload @workload, 0, @workload_timesteps
        solve_subset queries, indexes, data, reduced_workload, 0, @workload_timesteps, ts_indexes, ts_query_indexes

        # Construct and solve the ILP
        problem = TimeDependIterativeProblem.new(queries, @workload, data, ts_indexes, ts_query_indexes, @objective)
        problem.add_whole_step_constraints

        problem.solve

        # Return the selected indices
        selected_indexes = problem.selected_indexes

        problem.result
      end

      private

      def solve_subset(queries, indexes, data, workload, start_ts, end_ts, ts_indexes, ts_query_indexes)
        puts "-=-=start : " + start_ts.to_s + " end: "  + end_ts.to_s
        middle_ts = ((end_ts - start_ts) / 2).ceil + start_ts

        problem = TimeDependIterativeProblem.new(queries, workload, data,
                                                 ts_indexes, ts_query_indexes, @objective)
        problem.start_ts = start_ts
        problem.middle_ts = middle_ts
        problem.end_ts = end_ts
        problem.add_iterative_constraints

        STDERR.puts "optimization for #{start_ts} - #{end_ts}"
        problem.solve
        result = setup_result problem.result, data, @update_plans
        setup_fixed_hash result, queries, ts_indexes, ts_query_indexes, [start_ts, middle_ts, end_ts]

        left_workload = get_subset_workload workload, start_ts, middle_ts
        unless left_workload.nil?
          solve_subset(queries, indexes, data, left_workload, start_ts, middle_ts, ts_indexes, ts_query_indexes)
        end
        right_workload = get_subset_workload workload, middle_ts, end_ts
        unless right_workload.nil?
          solve_subset(queries, indexes, data, right_workload, middle_ts, end_ts, ts_indexes, ts_query_indexes)
        end
      end

      def setup_fixed_hash(result, queries, ts_indexes, ts_query_indexes, tss)
        result.indexes.zip(tss) do |indexes_ts|
          ts = indexes_ts.last
          ts_indexes[ts] = Set.new if ts_indexes[ts].nil?
          indexes_ts.first.each {|idx| ts_indexes[ts].add(idx)}
        end

        queries.select{|q| q.instance_of? Query}.product(tss).each do |query, ts|
          ts_query_indexes[ts] = {} if ts_query_indexes[ts].nil?
          ts_query_indexes[ts][query] = Set.new if ts_query_indexes[ts][query].nil?
          result.query_indexes[query][tss.index(ts)].each do |idx|
            ts_query_indexes[ts][query].add(idx)
          end
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
          freq = @workload.statement_weights[statement]
                     .select.with_index { |_, idx| [start_ts, middle_ts, end_ts].include? idx}
                     .map{|f, _| f}
          sub_workload.add_statement(statement, frequency: freq)
        end
        sub_workload
      end
    end
  end
end
