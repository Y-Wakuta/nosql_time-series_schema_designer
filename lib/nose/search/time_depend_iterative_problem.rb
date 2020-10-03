# frozen_string_literal: true

require 'logging'

require 'mipper'
begin
  require 'mipper/cbc'
rescue LoadError
  # We can't use most search functionality, but it won't explode
  nil
end

module NoSE
  module Search
    # A representation of a search problem as an ILP
    class TimeDependIterativeProblem < TimeDependProblem
      attr_accessor :ts_indexes, :ts_query_indexes, :start_ts, :middle_ts, :end_ts

      def initialize(queries, workload, data, ts_indexes,
                     ts_query_indexes, objective = nil)
        @start_ts = start_ts
        @middle_ts = middle_ts
        @end_ts = end_ts
        @ts_indexes = ts_indexes
        @ts_query_indexes = ts_query_indexes
        super(queries, workload, data, objective)
      end

      # @return [void]
      def add_iterative_constraints
        constraints = [
            TimeDependIndexFixConstraints,
        ]

        Parallel.each(constraints, in_threads: 2) { |constraint| constraint.apply self }
      end

      def add_whole_step_constraints
        constraints = [
            TimeDependIndexWholeFixConstraints
        ]

        Parallel.each(constraints, in_threads: 2) { |constraint| constraint.apply self }
      end
    end
  end
end
