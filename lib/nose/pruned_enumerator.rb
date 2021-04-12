# frozen_string_literal: true

require 'logging'
require 'fpgrowth'


module NoSE
  # Produces potential indices to be used in schemas
  class PrunedIndexEnumerator < IndexEnumerator
    def initialize(workload, cost_model, is_entity_fields_shared_threshold,
                   index_plan_step_threshold, index_plan_shared_threshold, choice_limit_size: 3_000)
      @eq_threshold = 1
      @cost_model = cost_model
      @is_entity_fields_shared_threshold = is_entity_fields_shared_threshold
      @index_steps_threshold = index_plan_step_threshold
      @index_plan_shared_threshold = index_plan_shared_threshold
      @choice_limit_size = choice_limit_size
      super(workload)
    end

    def indexes_for_queries(queries, additional_indexes)
      return [] if queries.empty?
      entity_fields = get_frequent_entity_choices queries
      puts "entity_fields (pattern) size: " + entity_fields.size.to_s
      extra_fields = get_frequent_extra_choices queries

      indexes = Parallel.flat_map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        idxs = indexes_for_query(query, entity_fields[query], extra_fields).to_a

        puts query.comment + ": " + idxs.size.to_s if query.instance_of? Query
        idxs
      end.uniq
      indexes += additional_indexes

      puts "index size before pruning: " + indexes.size.to_s
      if queries.all? {|q| q.instance_of? Query}
        indexes = pruning_tree_by_is_shared(queries, indexes)
        indexes += queries.map(&:materialize_view)
        indexes += queries.map(&:materialize_view_with_aggregation)
      else
        indexes = get_used_indexes(queries, indexes)
      end

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

    # Remove CFs based on whether the CF is share with other queries or not
    def pruning_tree_by_is_shared(queries, indexes)
      trees = get_trees queries, indexes
      show_tree trees

      upserts = @workload.statement_weights.keys.select{|s| s.is_a?(Insert) or s.is_a?(Update)}
      fixed_indexes = []
      trees.each do |tree|
        fixed_indexes << tree.query.materialize_view

        other_tree_indexes = trees.reject{|t| t == tree}.map{|t| t.flat_map(&:indexes).uniq}
        tree.each do |plan|
          # not-shared join plans are still worthy when any upsearts modifies its field.
          is_modified = upserts.any? do |upsert|
            plan.indexes.any? do |index|
              upsert.modifies_index? index
            end
          end
          # MV plan is not worthy if it is upserted, since there is MV plan.
          if is_modified and plan.indexes.size > 1
            fixed_indexes += plan.indexes
            next
          end

          # if the included indexes are not shared with other queries, the plan would not be recommended
          is_shared_than_threshold = plan.indexes.any? do |index|
            other_tree_indexes.count{|oti| oti.include?(index)} >= @index_plan_shared_threshold
          end
          if is_shared_than_threshold
            fixed_indexes += plan.indexes
            next
          end
        end
      end
      fixed_indexes.uniq
    end

    def show_tree(trees)
      trees.each do |tree|
        join_plan_size = tree.select{|p| p.indexes.size > 1}.size
        puts "--- #{tree.to_a.size} plans : #{join_plan_size} join plans for #{tree.query.text}  ---" if tree.query.instance_of? Query
      end
    end

    def get_trees(queries, indexes)
      planner = Plans::PrunedQueryPlanner.new @workload.model, indexes, @cost_model, @index_steps_threshold
      #planner = Plans::QueryPlanner.new @workload.model, indexes, @cost_model
      Parallel.map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        planner.find_plans_for_query(query)
      end
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query, entity_fields, extra_fields)
      @logger.debug "Enumerating indexes for query #{query.text}"

      #unless has_upsearted_entity? query
      #  return [query.materialize_view]
      #end

      range = get_query_range query
      eq = get_query_eq query

      # yusuke そもそもここの graph.subgraphs を1分割までにすればいいのでは? クエリプランで二つまでのジョインのみを許すことに一致している
      indexes = Parallel.flat_map(query.graph.subgraphs(), in_processes: [Parallel.processor_count - 5, 0].max()) do |graph|
        indexes_for_graph graph, query.select, eq, range,  entity_fields, extra_fields
      end.uniq
      indexes = ignore_cluster_key_order query, indexes
      indexes << query.materialize_view
      indexes << query.materialize_view_with_aggregation
      indexes
    end

    def ignore_cluster_key_order(query, indexes)
      pruned_indexes = indexes.sort_by{|i| i.hash_str}.dup
      indexes.each_with_index do |target_index, idx|
        next unless target_index.key_fields.size > (query.eq_fields + query.order).size
        indexes[(idx + 1)..-1].select{|i| target_index.hash_fields == i.hash_fields and \
                           target_index.order_fields.to_set == i.order_fields.to_set and \
                           target_index.extra == i.extra}.each do |other_index|
          worth_order_fields_candidates = ((query.eq_fields + query.order + query.groupby + [query.range_field]).to_set & target_index.key_fields).to_set
          variable_order_fields_size = [worth_order_fields_candidates.size - target_index.hash_fields.size, 0].max
          if target_index.order_fields.take(variable_order_fields_size) == other_index.order_fields.take(variable_order_fields_size)
            pruned_indexes = pruned_indexes.reject{|pi| pi == other_index}
          end
        end
      end
      pruned_indexes
    end

    # Produce the indexes necessary for support queries for these indexes
    # @return [Array<Index>]
    def support_indexes(indexes, by_id_graph)
      STDERR.puts "start enumerating support indexes"
      ## If indexes are grouped by ID graph, convert them before updating
      ## since other updates will be managed automatically by index maintenance
      indexes = indexes.map(&:to_id_graph).uniq if by_id_graph

      queries = support_queries indexes
      puts "support queries: " + queries.size.to_s
      support_indexes = indexes_for_queries queries, []
      STDERR.puts "end enumerating support indexes"
      support_indexes
    end

    private

    def has_upsearted_entity?(query)
      upsearts = @workload.statement_weights.keys.reject{|k| k.instance_of? Query}
      upsearts.any? do |upsts|
        query.graph.entities.include? upsts.entity
      end
    end

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, eq, range, entity_fields_patterns, extra_fields)
      eq_choices, order_choices, extra_choices = get_choices graph, select, eq, range, entity_fields_patterns, extra_fields

      ## Generate all possible indices based on the field choices
      choices = eq_choices.product(extra_choices)

      choices_limit_size = @choice_limit_size
      if choices.size > choices_limit_size
        STDERR.puts "pruning choices from #{choices.size.to_s} to #{choices_limit_size}"

        # sort choices to always get the same reduced-choices
        choices = choices.sort_by do |choice|
          (choice.first.map(&:id) + choice.last.map(&:id)).join(',')
        end.take(choices_limit_size)
      end

      indexes_for_choices(graph, choices, order_choices).uniq
    end

    def get_choices(graph, select, eq, range, entity_fields_patterns, extra_fields)
      eq_choices = frequent_eq_choices graph, entity_fields_patterns
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+).uniq
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = frequent_extra_choices(graph, select, eq, range, extra_fields)

      [eq_choices, order_choices, extra_choices]
    end

    def get_frequent_entity_choices(queries)
      entity_fields = get_entity_fields queries
      unique_shared_fields = split_entity_fields_into_unique_shared entity_fields
      frequent_shared_patterns = get_frequent_shared_patterns unique_shared_fields, queries

      reduced_fields = unique_shared_fields.dup.map do |query, fields|
        current_frequent_shared_patterns = frequent_shared_patterns.select{|fsp| fields[:shared].to_set >= fsp.content.to_set}
        threshold = current_frequent_shared_patterns.size > 1 ? [current_frequent_shared_patterns.map{|cfs| cfs.support}.max, 1].max : 1
        fields[:shared] = reduce_choices fields[:shared], current_frequent_shared_patterns, threshold
        Hash[query, fields]
      end.inject(:merge)

      validate_unique_shared_fields reduced_fields
      reduced_fields
    end

    def get_entity_fields(queries)
      queries.map do |query|
        eq = get_query_eq query
        Hash[query,
             query.graph.subgraphs.flat_map do |subgraph|
               subgraph.entities.flat_map do |entity|
                 # Get the fields for the entity and add in the IDs
                 entity_field = eq[entity] << entity.id_field
                 entity_field.uniq!
                 1.upto(entity_field.count).flat_map do |n|
                   entity_field.permutation(n).to_a
                 end
               end
             end.uniq # how many fields the one query has does not matter.
        ]
      end.inject(:merge)
    end

    def split_entity_fields_into_unique_shared(entity_fields)
      fields_hash = {}
      entity_fields.each do |_, efs|
        efs.each {|ef| fields_hash.has_key?(ef) ? fields_hash[ef] += 1 : fields_hash[ef] = 0}
      end
      entity_fields.map do |q, efs|
        shared = []
        unique = []
        efs.each {|ef| fields_hash[ef] > @is_entity_fields_shared_threshold ? shared << ef : unique << ef}
        Hash[q, {unique: unique, shared: shared}]
      end.inject(:merge)
    end

    def get_frequent_shared_patterns(unique_shared_fields, queries)
      shared_fields = unique_shared_fields.map{|_, usf| usf[:shared]}
      support_threshold = [2, queries.size].min()
      FpGrowth.mine(shared_fields.map(&:dup), 30)
          .select{|ef| ef.support >= support_threshold and ef.content.size > 1}
    end

    # every fields should be split into unique or shared.
    def validate_unique_shared_fields(unique_shared_fields)
      return unless unique_shared_fields.select{|_, usf| usf[:unique].empty? and usf[:shared].empty?}.size > 0
      fail 'some unique_shared_fields does not have any item'
    end

    def get_frequent_extra_choices(queries)
      extras = queries.map do |query|
        eq = get_query_eq query
        range = get_query_eq query
        query.graph.subgraphs.flat_map do |subgraph|
          extra_choices subgraph, query.select, eq, range
        end
      end
      FpGrowth.mine(extras).select{|ef| ef.support > 1}
    end

    def frequent_eq_choices(graph, entity_fields_patterns)
      # filter not used fields for this sub-graph
      shared_patterns = entity_fields_patterns[:shared].select{|efs| efs.all?{|ef| graph.entities.include? ef.parent}}
      shared_patterns += get_additional_shared_patterns_for_small_graph graph, shared_patterns, entity_fields_patterns

      # get each enumeration combination size for shared fields and unique fields
      graph_entities_size_for_shared = shared_patterns.flatten.map(&:parent).uniq.size
      graph_entities_size_for_unique = graph.entities.length - graph_entities_size_for_shared
      unique_patterns = entity_fields_patterns[:unique].select{|efpu| efpu.all?{|efp| graph.entities.include? efp.parent}}
      unique_eq_choices = enumerate_unique_patterns unique_patterns, graph_entities_size_for_unique

      eq_choices = 1.upto(graph_entities_size_for_shared).flat_map do |n|
        shared_eq_choices = shared_patterns.permutation(n).to_a << []
        (shared_eq_choices.product(unique_eq_choices) + unique_eq_choices.product(shared_eq_choices)).map{|f| f.flatten.uniq} + shared_eq_choices.map(&:flatten)
      end + unique_eq_choices

      eq_choices += enumerate_small_eq_choices shared_patterns, unique_patterns
      eq_choices = eq_choices.uniq.reject{|ec| ec.empty?}
      valid_eq_choices = eq_choices.select{|eq_choice| graph.entities >= eq_choice.map{|ec| ec.parent}.to_set}
      valid_eq_choices
    end

    # frequent entity_choices are combined. However, this result in no eq_choices for small sub-graphs
    # Only the reduced, in other words combined, field set have fields from more than two entities
    # TODO: This code ignoring combined choices for small query graphs. Replace this logic to more sophisticated one
    def get_additional_shared_patterns_for_small_graph(graph, shared_patterns, entity_fields_patterns)
      return [] if shared_patterns.size > 0 or graph.entities.size > 3
      entity_fields_patterns[:shared]
          .select{|efs| efs.map(&:parent).uniq.size > 1}
          .map{|efs| efs.select{|f| graph.entities.include? f.parent}}
    end

    def enumerate_small_eq_choices(shared_patterns, unique_patterns)
      1.upto(2).flat_map do |n|
        additional_eqs = (shared_patterns + unique_patterns).permutation(n).to_a.map(&:flatten).map(&:uniq).uniq
        additional_eqs.flat_map{|aeps| enumerate_by_non_primary_fields aeps}
      end.uniq
    end

    def enumerate_unique_patterns(unique_patterns, graph_entity_size_for_unique)
      # unique fields is not shared with other queries and no need to enumerate subset of unique fields
      0.upto([graph_entity_size_for_unique, 1].max()).flat_map do |n|
        unique_patterns.combination(n)
            .map(&:flatten)
            .map(&:uniq)
            .map{|e| e.sort_by { |b | [-b.cardinality, b.name] }}.uniq
      end
    end

    def frequent_extra_choices(graph, select, eq, range, extra_fields)
      extra_choices = extra_choices(graph, select, eq, range)
      current_extra_fields = extra_fields.select{|extra_field| extra_field.content.all?{|c| extra_choices.include? c}}
      frequent_extra_fields = extra_choices
      max_frequency = current_extra_fields.map(&:support).max()
      #frequent_extra_fields = reduce_choices(extra_choices, current_extra_fields, max_frequency).map{|c| c.flatten(1).uniq}
      extra_choices = 1.upto(frequent_extra_fields.length).flat_map do |n|
        frequent_extra_fields.combination(n).map{|fef| fef.flatten(1).uniq}
      end.map(&:to_set).uniq

      valid_extra_choices = extra_choices.select{|extra_choice| graph.entities >= extra_choice.map{|ec| ec.parent}.to_set}
      valid_extra_choices
    end

    # TODO: At least for extra_fields, reduce for all patterns reduces candidates too much. I need to used only effective sets
    # group choices if the choices are included in the same frequent pattern
    def reduce_choices(entity_choice, patterns, threshold)
      frequent_fields_set = patterns.select{|p| p.support >= threshold}
                                .sort_by { |p| [-p.content.size, p.support]}
                                .map(&:content)
                                .first
      return entity_choice if frequent_fields_set.nil?
      entity_choice = (entity_choice - frequent_fields_set) + [frequent_fields_set.flatten.uniq]
      entity_choice
    end

    def enumerate_by_primary_fields(fields)
      primary_fields = fields.select{|f| f.primary_key?}.permutation.to_a
      non_primary_fields = fields.select{|f| !f.primary_key?}.sort_by { |fp| [-fp.cardinality, fp.name]}
      primary_fields.product(non_primary_fields).map(&:flatten)
    end

    def enumerate_by_non_primary_fields(fields)
      primary_fields = fields.select{|f| f.primary_key?}.sort_by { |fp| [-fp.cardinality, fp.name]}
      non_primary_fields = fields.select{|f| !f.primary_key?}.permutation.to_a
      non_primary_fields.product(primary_fields).map(&:flatten)
    end
  end
end

