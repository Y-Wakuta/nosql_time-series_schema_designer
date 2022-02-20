# frozen_string_literal: true

require 'logging'

module NoSE
  # Produces potential indices to be used in schemas
  class GraphBasedIndexEnumerator < IndexEnumerator
    def initialize(workload, cost_model, index_plan_step_threshold, choice_limit_size)
      @eq_threshold = 1
      @cost_model = cost_model
      @index_steps_threshold = index_plan_step_threshold
      @choice_limit_size = choice_limit_size
      super(workload)
    end

    def indexes_for_queries(queries, additional_indexes)
      return [] if queries.empty?

      indexes = Parallel.flat_map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        indexes_for_query(query).to_a
      end.uniq
      indexes += additional_indexes

      puts "index size before pruning: " + indexes.size.to_s
      indexes = get_used_indexes(queries, indexes)
      puts "index size after pruning: " + indexes.size.to_s

      indexes.uniq
    end

    # remove CFs that are not used in query plans
    def get_used_indexes(queries, indexes)
      puts "whole indexes : " + indexes.size.to_s
      get_trees(queries, indexes).flat_map do |tree|
        tree.flat_map(&:indexes).uniq
      end.uniq
    end

    def get_trees(queries, indexes)
      planner = Plans::PrunedQueryPlanner.new @workload.model, indexes, @cost_model, @index_steps_threshold
      Parallel.map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        planner.find_plans_for_query(query)
      end
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query
      orderby = get_query_orderby query

      indexes = group_subgraph(query.eq_fields, query.graph).flat_map do |subgraph_pair|
        overlapping_entities = subgraph_pair[:prefix].entities & subgraph_pair[:suffix].entities

        prefix_idxes = indexes_for_graph(subgraph_pair[:prefix], query.select, eq, range, orderby,
                                         overlapping_entities, is_prefix_graph: true)

        if subgraph_pair[:prefix].entities.size == 1 && subgraph_pair[:suffix].entities == query.graph.entities
          suffix_idxes = indexes_for_full_suffix_graph(subgraph_pair[:prefix], subgraph_pair[:suffix],
                                                       query.materialize_view, eq)
        else
          suffix_idxes = indexes_for_graph(subgraph_pair[:suffix], query.select, eq,
                                           range, orderby, overlapping_entities, is_prefix_graph: false)
        end

        [prefix_idxes + suffix_idxes].flatten
      end
      indexes.uniq!
      index_size = indexes.size
      indexes = ignore_cluster_key_order query, indexes
      puts "prune indexes based on clustering key #{index_size} -> #{indexes.size}"
      indexes << query.materialize_view
      indexes << query.materialize_view_with_aggregation
      puts "#{indexes.size} indexes for #{query.comment}"
      indexes
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

    # enumerate CFs by changing the field order of the first entity of MV
    def indexes_for_full_suffix_graph(prefix_graph, suffix_graph, materialize_view, eq)
      prefix_eq_choices = eq_choices prefix_graph, eq
      prefix_eq_choices.map do |eq_choice|
        generate_index(eq_choice, materialize_view.order_fields.reject{|of| eq_choice.include? of},
                       materialize_view.extra.reject{|e| eq_choice.include? e}, suffix_graph)
      end.compact
    end

    # get subgraph pairs with prefix-graph and suffix-graph
    def group_subgraph(eq, parent_graph)
      subgraphs = parent_graph.subgraphs(recursive = false).to_a
      subgraph_pairs = []
      subgraphs.each_with_index do |subgraph, idx|
        subgraphs[(idx + 1)..-1].each do |other_subgraph|
          next unless (subgraph.entities & other_subgraph.entities).size == 1
          next unless (subgraph.entities | other_subgraph.entities) == parent_graph.entities
          next if subgraph.entities.size == 1 && \
                  !(other_subgraph.join_order(eq).take(1) == subgraph.join_order(eq) || \
                  other_subgraph.join_order(eq).reverse.take(1) == subgraph.join_order(eq))
          next if other_subgraph.entities.size == 1 && \
                  !(subgraph.join_order(eq).take(1) == other_subgraph.join_order(eq) || \
                  subgraph.join_order(eq).reverse.take(1) == other_subgraph.join_order(eq))
          next if (subgraph.entities.size == 1 && !eq.map(&:parent).include?(subgraph.entities.first)) || \
                  (other_subgraph.entities.size == 1 && !eq.map(&:parent).include?(other_subgraph.entities.first))

          prefix_subgraph, suffix_subgraph = subgraph, other_subgraph
          if suffix_subgraph.entities.size == 1 && prefix_subgraph.entities.size > 1
            prefix_subgraph, suffix_subgraph = suffix_subgraph, prefix_subgraph
          end
          # choose prefix subgraph. this works only when parent_graph == query.graph
          unless eq.map(&:parent).include? prefix_subgraph.join_order(eq).first
            prefix_subgraph, suffix_subgraph = suffix_subgraph, prefix_subgraph
          end

          subgraph_pairs << {prefix: prefix_subgraph, suffix: suffix_subgraph}
        end
      end
      subgraph_pairs
    end

    def indexes_for_graph(graph, select, eq, range, orderby, overlapping_entities, is_prefix_graph: true)
      eq_choices, order_choices, extra_choices = get_graph_choices graph, select, eq, range, orderby, overlapping_entities, is_prefix_graph: is_prefix_graph
      choices = eq_choices.product(extra_choices)

      choices = limit_choices choices
      indexes_for_choices(graph, choices, order_choices).uniq
    end

    def support_indexes(indexes, by_id_graph)
      STDERR.puts "start enumerating support indexes"
      indexes = indexes.map(&:to_id_graph).uniq if by_id_graph

      queries = support_queries indexes
      puts "support queries: " + queries.size.to_s
      support_indexes = indexes_for_queries queries, []
      STDERR.puts "end enumerating support indexes"
      support_indexes
    end

    private

    def limit_choices(choices)
      return choices if @choice_limit_size.nil? || choices.size < @choice_limit_size

      base_size = choices.size
      # sort choices to always get the same reduced-choices
      choices = choices.sort_by do |choice|
        (choice.first.map(&:id) + choice.last.map(&:id)).join(',')
      end.take(@choice_limit_size)
      STDERR.puts "pruning choices from #{base_size} to #{choices.size}"
      choices
    end

    def get_graph_choices(graph, select, eq, range, orderby, overlapping_entities, is_prefix_graph: true)
      eq_choices = eq_choices graph, eq
      eq_choices = is_prefix_graph ?
                     prune_eq_choices_for_prefix_graph(eq_choices, eq, range, orderby)
                     : prune_eq_choices_for_suffix_graph(eq_choices, overlapping_entities)

      order_choices = order_choices(graph, range, is_prefix_graph)
      extra_choices = extra_choices(graph, select, eq, range)

      [eq_choices, order_choices, extra_choices]
    end

    def order_choices(graph, range, is_prefix_graph)
      return [[]] if is_prefix_graph or range.keys.to_set < graph.entities
      # order by is not executed partially.
      # Thus, This enumerator only enumerates order fields for graph that has all of required entity
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+).uniq
      order_choices = range_fields.permutation.to_a << []
      order_choices
    end

    def prune_eq_choices_for_prefix_graph(eq_choices, eq, range, orderby)
      eq_entities = eq.keys
      eq_choices = eq_choices.select do |eq_choice|
        eq_choice_entities = eq_choice.map(&:parent).uniq
        key_prefix_entities = (eq_entities & eq_choice_entities).uniq
        next false if key_prefix_entities.empty?

        # check is any of eq.keys matches overlapping_eq_entities
        # eq_choice_entities start with query-key specified entities
        overlapping_eq_entities = eq_choice_entities.take(key_prefix_entities.size).to_set
        eq_entities.to_set >= overlapping_eq_entities
      end

      # As long as each field of eq_choices is not included in any of eq, range, orderby, groupby,
      # it does not have to be the first place of id_field
      eq_choices = eq_choices.select do |eq_choice|
        next true if eq_choice.size <= 1

        # Since this CF does not have aggregation, we don't care groupby below
        non_query_specified_id_fields = eq_choice.select(&:primary_key)
                                                 .reject{|f| eq.values.flatten.include?(f) ||
                                                   range.values.flatten.include?(f) ||
                                                   orderby.values.flatten.include?(f)}
        query_specified_fields = eq_choice.to_set - non_query_specified_id_fields.to_set
        next true if non_query_specified_id_fields.empty? || query_specified_fields.empty?
        query_specified_fields.map{|qsf| eq_choice.index(qsf)}.max < non_query_specified_id_fields.map{|nqsif| eq_choice.index(nqsif)}.min
      end

      eq_choices
    end

    def prune_eq_choices_for_suffix_graph(eq_choices, overlapping_entities)
      fail if overlapping_entities.size > 1

      # suffix graph cf should start with overlapping entity
      eq_choices = eq_choices.select do |eq_choice|
        eq_choice.map(&:parent).uniq.first == overlapping_entities.first
      end

      ## suffix eq_choices are enough with starting by primary keys
      eq_choices = eq_choices.select do |eq_choice|
        eq_choice.first.primary_key?
      end
      eq_choices
    end

    def get_query_orderby(query)
      order = query.order.group_by(&:parent)
      order.default_proc = ->(*) { [] }
      order
    end
  end
end
