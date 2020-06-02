# frozen_string_literal: true

require 'logging'
require 'fpgrowth'

module NoSE
  # Produces potential indices to be used in schemas
  class PrunedIndexEnumerator < IndexEnumerator
    def initialize(workload, cost_model)
      @eq_threshold = 1
      @cost_model = cost_model
      super(workload)
    end

    def indexes_for_queries(queries, additional_indexes)
      return [] if queries.empty?
      entity_fields = get_frequent_entity_choices queries
      puts "entity_fields (pattern) size: " + entity_fields.size.to_s
      extra_fields = get_frequent_extra_choices queries
      #indexes = Parallel.map(queries, in_processes: 10) do |query|
      indexes = queries.map do |query|
        indexes_for_query(query, entity_fields[query], extra_fields).to_a
      end.inject(additional_indexes, &:+)

      get_used_indexes_in_query_plans queries, indexes
    end

    # remove CFs that are not used in query plans
    def get_used_indexes_in_query_plans(queries, indexes)
      puts "whole indexes : " + indexes.size.to_s
      planner = Plans::QueryPlanner.new @workload.model, indexes, @cost_model
      used_indexes = queries.flat_map do |query|
        plan = planner.find_plans_for_query(query)
        plan.flat_map{|p| p.steps.map{|s| s.index}}.uniq
      end
      puts "uesd indexes : " + used_indexes.size.to_s
      used_indexes
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query, entity_fields, extra_fields)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query

      #indexes = Parallel.flat_map(query.graph.subgraphs, in_processes: 7) do |graph|
      indexes = query.graph.subgraphs.flat_map do |graph|
        indexes_for_graph graph, query.select, query.counts, query.sums, query.maxes, query.avgs, eq, range, query.groupby, entity_fields, extra_fields
      end.uniq << query.materialize_view

      puts "pie == #{query.text} =============================== " + indexes.size.to_s
      indexes
    end

    private

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, count, sum, max, avg, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices, order_choices, extra_choices = get_choices graph, select, eq, range, group_by, entity_fields_patterns, extra_fields

      ## Generate all possible indices based on the field choices
      choices = eq_choices.product(extra_choices)
      indexes_for_choices(graph, count, sum, max, avg, group_by, choices, order_choices).uniq
    end

    def get_choices(graph, select, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices = frequent_eq_choices graph, group_by, entity_fields_patterns
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+).uniq
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = frequent_extra_choices(graph, select, eq, range, extra_fields)
      #puts("graph : #{graph.inspect}")
      puts("eq_choices : #{eq_choices.size}")
      puts("extra_choices : #{extra_choices.size}")

      [eq_choices, order_choices, extra_choices]
    end

    def get_frequent_entity_choices(queries)
      entity_fields = queries.map do |query|
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

      unique_shared_fields = entity_fields.map do |q, efs_target|
        tmp_efs = efs_target.dup
        entity_fields.reject{|q_, _| q == q_}.each do |_, efs|
          efs_target -= efs
        end
        Hash[q, {unique: efs_target, shared: tmp_efs - efs_target}]
      end.inject(:merge)

      shared_fields = unique_shared_fields.map{|_, usf| usf[:shared]}
      support_threshold = [2, queries.size].min() # allow support == 1 if there is only one query
      frequent_shared_patterns = FpGrowth.mine(shared_fields)
                                   .select{|ef| ef.support >= support_threshold and ef.content.size > 1}

      unique_shared_fields = unique_shared_fields.map do |query, fields|
        current_frequent_shared_patterns = frequent_shared_patterns.select{|fsp| fields[:shared].to_set >= fsp.content.to_set}
        fields[:shared] = reduce_choices fields[:shared], current_frequent_shared_patterns
        Hash[query, fields]
      end.inject(:merge)
      unique_shared_fields
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

    def frequent_eq_choices(graph, group_by, entity_fields_patterns)
      graph_entities_size_for_shared = entity_fields_patterns[:shared].flatten.map(&:parent).size
      graph_entities_size_for_unique = graph.entities.length - graph_entities_size_for_shared

      #unique_eq_choices = 1.upto(graph_entities_size_for_unique).flat_map do |n|
      #  entity_fields_patterns[:unique].combination(n).map(&:flatten).map{|e| e.sort_by { |b | -b.cardinality }}.uniq
      #end.uniq
      entity_fields_patterns[:unique] << []
      # unique fields is not shared with other queries and no need to enumerate subset of unique fields
      unique_eq_choices = entity_fields_patterns[:unique].combination([graph_entities_size_for_unique, 1].max())
                            .map(&:flatten)
                            .map(&:uniq)
                            .map{|e| e.sort_by { |b | [-b.cardinality, b.hash] }}.uniq
      #tmp_unique_eq_choices = entity_field_combinations(entity_fields_patterns[:unique]).map(&:flatten).map(&:uniq).map{|e| e.sort_by { |b | [-b.cardinality, b.hash] }}.uniq

      eq_choices = 2.upto(graph_entities_size_for_shared).flat_map do |n|
        (entity_fields_patterns[:shared] + [unique_eq_choices]).permutation(n).map(&:flatten).map(&:uniq).to_a
      end + [entity_fields_patterns[:shared].flatten.uniq] + unique_eq_choices

      unless group_by.empty?
        #eq_choices = prune_eq_choices_for_groupby eq_choices, group_by
      end

      eq_choices.reject{|ec| ec.empty?}
    end

    def frequent_extra_choices(graph, select, eq, range, extra_fields)
      extra_choices = extra_choices(graph, select, eq, range)
      current_extra_fields = extra_fields.select{|extra_field| extra_field.content.all?{|c| extra_choices.include? c}}
      #frequent_extra_fields = reduce_choices(extra_choices, current_extra_fields).map{|c| c.flatten(1).uniq}
      frequent_extra_fields = extra_choices
      extra_choices = 1.upto(frequent_extra_fields.length).flat_map do |n|
        frequent_extra_fields.combination(n).map{|fef| fef.flatten(1).uniq}
      end.map(&:to_set).uniq

      extra_choices
    end

    # TODO: At least for extra_fields, reduce for all patterns reduces candidates too much. I need to used only effective sets
    # group choices if the choices are included in the same frequent pattern
    def reduce_choices(entity_choice, patterns)
      patterns.map(&:content).sort_by { |c| -c.size}.each do |pattern|
        if entity_choice.to_set >= pattern.to_set
          entity_choice = (entity_choice - pattern).map{|f| [f]} + [pattern]
        end
      end
      entity_choice
    end

    def prune_eq_choices_for_groupby(eq_choices, group_by)
      # remove eq_choices that does not have group by fields
      eq_choices_with_groupby = eq_choices.select{|ec| ec.to_set >= group_by}
      eq_choices_without_groupby = eq_choices - eq_choices_with_groupby
      eq_choices_with_groupby = eq_choices_with_groupby.map do |eq_choice|
        eq_choice.reject!{|ec| group_by.include? ec}

        # force eq_choice to start from group by fields
        group_by.sort_by { |gb| gb.hash } + eq_choice
      end

      eq_choices_with_groupby + eq_choices_without_groupby
    end

    def get_median(a)
      a.sort!
      (a.size % 2).zero? ? a[a.size/2 - 1, 2].inject(:+) / 2.0 : a[a.size/2]
    end

    #def entity_field_combinations(entity_fields)
    #  return [] if entity_fields.size == 0
    #  entity_hash = {}
    #  entity_fields.reject{|efs| efs.empty?}.each do |efs|
    #    fail 'each entity fields should have only one entity' if efs.map(&:parent).uniq.size > 1
    #    unless entity_hash.has_key? efs.first.parent
    #      entity_hash[efs.first.parent] = []
    #      entity_hash[efs.first.parent] << []
    #    end
    #    entity_hash[efs.first.parent] << efs
    #  end
    #  entity_hash.values[0].product(*entity_hash.values[1..-1])
    #end

  end
end

