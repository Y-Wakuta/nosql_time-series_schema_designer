# frozen_string_literal: true

require 'logging'
require 'fpgrowth'

module NoSE
  # Produces potential indices to be used in schemas
  class PrunedIndexEnumerator < IndexEnumerator
    def initialize(workload)
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
        #indexes = queries.map do |query|
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
      threshold = 1
      entity_choices = graph.entities.flat_map do |entity|
        # Get the fields for the entity and add in the IDs
        entity_fields = eq[entity] << entity.id_field
        entity_fields.uniq!
        # get entity fields that has frequent entity fields
        frequent_patterns = entity_fields_patterns.select{|efp| entity_fields.to_set >= efp.content.to_set \
                                            and efp.support > threshold}
        whole_fields = entity_fields_patterns.find{|efp| entity_fields.to_set == efp.content.to_set}
        frequent_patterns + [whole_fields]
      end

      # sort entity choices by the support of each entity choices
      whole_eq_choices = entity_choices.sort_by{|ec| -ec.support}
                             .map(&:content)
                             .flatten(1)
                             .uniq

      eq_choices = 1.upto(graph.entities.length).map do |n|
        eq_choice = []
        whole_eq_choices.each do |wec|
          tmp_eq_choice = eq_choice.dup
          tmp_eq_choice.push wec
          if tmp_eq_choice.map(&:parent).uniq.size > n
            break
          end
          eq_choice = tmp_eq_choice.dup
        end
        eq_choice
      end.uniq

      unless group_by.empty?
        eq_choices = prune_eq_choices_for_groupby eq_choices, group_by
      end

      eq_choices
    end

    def get_choices(graph, select, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices = eq_choices graph, eq, group_by, entity_fields_patterns
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+)
      range_fields.uniq!
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = extra_choices graph, select, eq, range
      max_frequency = extra_fields.map{|ef| ef.support}.max()
      current_extra_fields = extra_fields
                                 .select{|ef| extra_choices
                                                  .any?{|ec| ec.to_set >= ef.content.to_set or ec.to_set <= ef.content.to_set} and ef.support > [1, max_frequency].max()}
                                 .map{|ef| ef.content}
      extra_choices += current_extra_fields
      [eq_choices, order_choices, extra_choices]
    end

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, count, sum, max, avg, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices, order_choices, extra_choices = get_choices graph, select, eq, range, group_by, entity_fields_patterns, extra_fields

      ## Generate all possible indices based on the field choices
      choices = eq_choices.product(extra_choices)
      indexes = indexes_for_choices(graph, count, sum, max, avg, group_by, choices, order_choices)

      additional_indexes = additional_indexes_4_eq_choices(graph, eq_choices)
      indexes += additional_indexes
      indexes.uniq!

      indexes
    end

    def additional_indexes_4_eq_choices(graph, key_choices)
      key_choices = key_choices.map do |key_choice|
        id_fields = key_choice.map{|ec| ec.parent.id_field}
        key_choice += id_fields
        key_choice.uniq
      end

      Parallel.map(key_choices.select{|kc| kc.size > 1}, in_processes: 4) do |key_choice|
        tmp = Parallel.map((2..key_choice.size), in_processes: 2) do |field_size|
          key_choice.combination(field_size).map do |all_fields|
            not_included_nodes = graph.nodes.reject{|n| all_fields.map(&:parent).include? n.entity}
            current_graph = graph.dup
            current_graph.remove_nodes(not_included_nodes)
            next unless current_graph.is_valid?

            indexes = (1...all_fields.size).map do |key_size|
              all_fields.combination(key_size).map do |hash_fields|
                order_fields = all_fields - hash_fields
                next unless order_fields.any?{|af| af.primary_key?}
                order_fields = order_fields.sort_by { |of | of.hash}
                hash_fields.permutation(hash_fields.size).map do |hash_field|
                  generate_index hash_field, order_fields, [], current_graph, Set.new, Set.new, Set.new, Set.new, Set.new
                end
              end.compact
            end
            indexes
          end
        end
        tmp
      end.flatten(5).uniq.compact.uniq
    end
  end
end

