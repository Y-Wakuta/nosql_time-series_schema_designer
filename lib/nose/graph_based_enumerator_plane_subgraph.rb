# frozen_string_literal: true

require 'logging'

module NoSE
  # Produces potential indices to be used in schemas
  class GraphBasedIndexEnumeratorWithPlaneSubgraph < GraphBasedIndexEnumerator

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query
      orderby = get_query_orderby query

      indexes = query.graph.subgraphs(recursive=false).flat_map do |graph|
        indexes_for_graph graph, query.select, eq, range, orderby, {}
      end.uniq
      indexes = ignore_cluster_key_order query, indexes
      indexes << query.materialize_view
      indexes << query.materialize_view_with_aggregation
      puts "#{indexes.size} indexes for #{query.comment}"
      indexes
    end

    private

    def get_graph_choices(graph, select, eq, range, orderby, overlapping_entities, is_prefix_graph: true)
      eq_choices = eq_choices graph, eq

      # order by is not executed partially.
      # Thus, This enumerator only enumerates order fields for graph that has all of required entity
      order_choices = order_choices(graph, range)
      extra_choices = extra_choices(graph, select, eq, range)

      [eq_choices, order_choices, extra_choices]
    end
  end
end
