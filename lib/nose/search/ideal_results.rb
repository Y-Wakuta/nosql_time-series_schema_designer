# frozen_string_literal: true

module NoSE
  module Search
    # A container for results from a schema search
    class IdealResults < TimeDependResults

      def initialize(results = [], problem = nil, by_id_graph = false)
        @problem= problem
        @timesteps = results.size
        @by_id_graph = by_id_graph
        @migrate_plans = []
        @time_depend_plans = []
        @each_indexes = []
        @indexes = results.map(&:indexes)
        @total_size = results.map(&:total_size)

        # Find the indexes the ILP says the query should use
        @query_indexes = Hash.new
        results.each_with_index do |result, ts|
          result.query_indexes.each do |q, each_indexes|
            @query_indexes[q] = {} if @query_indexes[q].nil?
            @query_indexes[q][ts] = Set.new if @query_indexes[q][ts].nil?
            @query_indexes[q][ts] = each_indexes
          end
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

        #check_indexes.each do |check_indexes_one_timestep|
        #  fail InvalidResultsException unless \
        #  (check_indexes_one_timestep - @enumerated_indexes).empty?
        #end

        #validate_migrate_plans
      end
    end
  end
end
