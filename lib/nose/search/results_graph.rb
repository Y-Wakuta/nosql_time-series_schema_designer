# frozen_string_literal: true
#
require_relative './results'

module NoSE
  module Search
    # A container for results from a schema search
    class ResultsGraph < Results

      def initialize(problem = nil, by_id_graph = false)
        @problem = problem
        return if problem.nil?
        @by_id_graph = by_id_graph

        # Find the indexes the ILP says the query should use
        @query_indexes = Hash.new { |h, k| h[k] = Set.new }

        @problem.edge_vars.each do |query, edge_var|
          edge_var.each do |from, edge|
            edge.each do |to, var|
              next unless var.value
             @query_indexes[query].add to
            end
          end
        end
      end
    end
  end
end
