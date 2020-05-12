# frozen_string_literal: true

require 'logging'
require 'fpgrowth'

module NoSE
  # Produces potential indices to be used in schemas
  class PrunedIndexEnumerator < IndexEnumerator
    def initialize(workload)
      @eq_threshold = 1
      super(workload)
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query, entity_fields: nil, extra_fields: nil)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query

      Parallel.flat_map(query.graph.subgraphs, in_processes: 7) do |graph|
        #query.graph.subgraphs.flat_map do |graph|
        indexes_for_graph graph, query.select, query.counts, query.sums, query.maxes, query.avgs, eq, range, query.groupby, entity_fields, extra_fields
      end.uniq << query.materialize_view
    end

    def indexes_for_queries(queries, additional_indexes)
      entity_fields = get_frequent_entity_choices queries
      extra_fields = get_frequent_extra_choices queries
      Parallel.map(queries) do |query|
        #queries.map do |query|
        indexes_for_query(query, entity_fields: entity_fields, extra_fields: extra_fields).to_a
      end.inject(additional_indexes, &:+)
    end

    private

    def get_frequent_entity_choices(queries)
      entity_fields = queries.map do |query|
        eq = get_query_eq query
        query.graph.subgraphs.map do |subgraph|
          subgraph.entities.flat_map do |entity|
            # Get the fields for the entity and add in the IDs
            entity_fields = eq[entity] << entity.id_field
            entity_fields.uniq
          end
        end
      end.flatten(1)
      FpGrowth.mine(entity_fields)
    end

    def get_frequent_extra_choices(queries)
      extras = queries.flat_map do |query|
        eq = get_query_eq query
        range = get_query_eq query
        extra_choices query.graph, query.select, eq, range
      end
      FpGrowth.mine(extras)
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

    def eq_choices(graph, eq, group_by, entity_fields_patterns)
      entity_choices = graph.entities.flat_map do |entity|
        # Get the fields for the entity and add in the IDs
        entity_fields = eq[entity] << entity.id_field
        entity_fields.uniq!
        # get entity fields that has frequent entity fields
        entity_fields_patterns.select{|efp| entity_fields.to_set >= efp.content.to_set}
      end.sort_by { |ec| -ec.content.size}

      frequent_entity_choices = entity_choices.dup
      entity_choices.each_with_index do |larger_entity_choice, larger_idx|
        entity_choices.slice((larger_idx+ 1)..-1).each do |entity_choice|
          if larger_entity_choice.support > @eq_threshold and larger_entity_choice.content.to_set >= entity_choice.content.to_set
            frequent_entity_choices.delete(entity_choice)
          end
        end
      end

      eq_choices = 2.upto(graph.entities.length).flat_map do |n|
        frequent_entity_choices.map{|fec| fec.content}.permutation(n).map(&:flatten).to_a
      end + entity_choices.map{|ec| ec.content}

      unless group_by.empty?
        eq_choices = prune_eq_choices_for_groupby eq_choices, group_by
      end

      eq_choices
    end

    def get_median(a)
      a.sort!
      (a.size % 2).zero? ? a[a.size/2 - 1, 2].inject(:+) / 2.0 : a[a.size/2]
    end

    def frequent_extra_choices(graph, select, eq, range, extra_fields)
      extra_choices = extra_choices(graph, select, eq, range)
      median_support = get_median(extra_fields.map{|ef| ef.support})
      puts("max frequency is #{median_support}")
      current_extra_fields = extra_fields
                                 .select{|ef| extra_choices
                                                  .any?{|ec| ec.to_set >= ef.content.to_set or ec.to_set <= ef.content.to_set} and ef.support > [1, median_support].max()}
                                 .map{|ef| ef.content}
      extra_choices + current_extra_fields
    end

    def get_choices(graph, select, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices = eq_choices graph, eq, group_by, entity_fields_patterns
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+)
      range_fields.uniq!
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = frequent_extra_choices(graph, select, eq, range, extra_fields)

      [eq_choices, order_choices, extra_choices]
    end

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, count, sum, max, avg, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices, order_choices, extra_choices = get_choices graph, select, eq, range, group_by, entity_fields_patterns, extra_fields

      ## Generate all possible indices based on the field choices
      choices = eq_choices.product(extra_choices)
      indexes = indexes_for_choices(graph, count, sum, max, avg, group_by, choices, order_choices)
      indexes.uniq!

      indexes
    end
  end
end

