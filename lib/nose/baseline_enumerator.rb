# frozen_string_literal: true

require 'logging'

module NoSE
  # Produces potential indices to be used in schemas
  class BaseLineIndexEnumerator < IndexEnumerator
    def initialize(workload, cost_model)
      @logger = Logging.logger['nose::enumerator']

      @workload = workload
      @cost_model = cost_model
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query

      indexes = Parallel.flat_map(query.graph.subgraphs, in_processes: 5) do |graph|
        indexes_for_graph graph, query.select, eq, range
      end.uniq << query.materialize_view << query.materialize_view_with_aggregation

      ignore_cluster_key_order query, indexes
    end

    def ignore_cluster_key_order(query, indexes)
      overlapping_index_keys = []
      condition_fields = (query.eq_fields + query.order.to_set + query.range_fields.to_set).reject(&:nil?)
      indexes.sort_by!(&:hash_str)
      indexes.each_with_index do |base_index, idx|
        next if overlapping_index_keys.include? base_index.key # this cf is already removed
        next if (base_index.key_fields.to_set - (query.eq_fields + query.order).to_set).empty?
        cf_condition_fields = condition_fields.select{|f| query.graph.entities.include? f.parent}.to_set

        query_condition_order_fields = ((cf_condition_fields - base_index.hash_fields) & base_index.order_fields).to_set
        variable_order_fields_size = (query_condition_order_fields.to_set & base_index.order_fields.to_set).size

        # TODO: we still distinguish the boundary between suffix order fields and extra,
        # TODO: but this does not affect performance and there are still some space to increase similar_indexes
        similar_indexes = indexes[(idx + 1)..-1].select{|i| base_index.hash_fields == i.hash_fields}
                            .select{|i| base_index.order_fields.take(variable_order_fields_size) \
                                          == i.order_fields.take(variable_order_fields_size)}
                            .select{|i| base_index.order_fields[variable_order_fields_size..].to_set + base_index.extra \
                                          == i.order_fields[variable_order_fields_size..].to_set + i.extra}
                            .reject{|i| overlapping_index_keys.include? i.key}
        overlapping_index_keys += similar_indexes.map(&:key)
      end
      indexes.reject { |i| overlapping_index_keys.include? i.key}
    end

    # remove CFs that are not used in query plans
    def get_used_indexes(queries, indexes)
      puts "whole indexes : " + indexes.size.to_s
      get_trees(queries, indexes).flat_map do |tree|
        tree.flat_map(&:indexes).uniq
      end.uniq
    end

    def get_trees(queries, indexes)
      planner = Plans::PrunedQueryPlanner.new @workload.model, indexes, @cost_model, 2
      #planner = Plans::QueryPlanner.new @workload.model, indexes, @cost_model
      Parallel.map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        planner.find_plans_for_query(query)
      end
    end

    def indexes_for_queries(queries, additional_indexes)
      indexes = Parallel.flat_map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        #indexes = queries.flat_map do |query|
        indexes_for_query(query).to_a
      end.uniq + additional_indexes

      puts "index size before pruning: " + indexes.size.to_s
      indexes = get_used_indexes(queries, indexes)
      puts "index size after pruning: " + indexes.size.to_s

      indexes.uniq
    end

  end
end
