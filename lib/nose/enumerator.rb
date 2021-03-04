# frozen_string_literal: true

require 'logging'

module NoSE
  # Produces potential indices to be used in schemas
  class IndexEnumerator
    def initialize(workload)
      @logger = Logging.logger['nose::enumerator']

      @workload = workload
    end

    def get_query_eq(query)
      eq = query.eq_fields.group_by(&:parent)
      eq.default_proc = ->(*) { [] }
      eq
    end

    def get_query_range(query)
      range = if query.range_field.nil?
                query.order
              else
                [query.range_field] + query.order
              end
      range = range.group_by(&:parent)
      range.default_proc = ->(*) { [] }
      range
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query

      Parallel.flat_map(query.graph.subgraphs, in_processes: 5) do |graph|
        #query.graph.subgraphs.flat_map do |graph|
        indexes_for_graph graph, query.select, eq, range
      end.uniq << query.materialize_view
    end

    def indexes_for_queries(queries, additional_indexes)
      Parallel.flat_map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        #indexes = queries.flat_map do |query|
        indexes_for_query(query).to_a
      end.uniq + additional_indexes
    end

    # Produce all possible indices for a given workload
    # @return [Set<Index>]
    def indexes_for_workload(additional_indexes = [], by_id_graph = false)
      queries = @workload.queries
      indexes = indexes_for_queries queries, additional_indexes
      #puts("basic query enumeration done : " + indexes.size.to_s)

      # Add indexes generated for support queries
      supporting = support_indexes indexes, by_id_graph
      supporting += support_indexes supporting, by_id_graph
      indexes += supporting
      puts("support query enumeration done")

      # Deduplicate indexes, combine them and deduplicate again
      indexes.uniq!
      combine_indexes indexes
      indexes.uniq!

      puts "# of basic indexes is #{indexes.size}"

      @logger.debug do
        "Indexes for workload:\n" + indexes.map.with_index do |index, i|
          "#{i} #{index.inspect}"
        end.join("\n")
      end

      indexes
    end

    protected

    # Produce the indexes necessary for support queries for these indexes
    # @return [Array<Index>]
    def support_indexes(indexes, by_id_graph)
      # If indexes are grouped by ID graph, convert them before updating
      # since other updates will be managed automatically by index maintenance
      indexes = indexes.map(&:to_id_graph).uniq if by_id_graph

      queries = support_queries indexes
      indexes_for_queries queries, []
    end

    # Collect all possible support queries
    def support_queries(indexes)
      # Enumerate indexes for each support query
      Parallel.flat_map(indexes, in_processes: [Parallel.processor_count - 4, 0].max()) do |index|
        #indexes.flat_map do |index|
        @workload.updates.flat_map do |update|
          update.support_queries(index)
        end
      end.uniq
    end

    private

    # Combine the data of indices based on matching hash fields
    def combine_indexes(indexes)
      no_order_indexes = indexes.select do |index|
        index.order_fields.empty?
      end
      no_order_indexes = no_order_indexes.group_by do |index|
        [index.hash_fields, index.graph]
      end

      no_order_indexes.each do |(hash_fields, graph), hash_indexes|
        extra_choices = hash_indexes.map(&:extra).uniq

        # XXX More combos?
        combos = extra_choices.combination(2)

        combos.map do |combo|
          indexes << Index.new(hash_fields, [], combo.inject(Set.new, &:+),
                               graph)
          @logger.debug "Enumerated combined index #{indexes.last.inspect}"
        end
      end
    end

    # Get all possible choices of fields to use for equality
    # @return [Array<Array>]
    def eq_choices(graph, eq)
      entity_choices = graph.entities.flat_map do |entity|
        # Get the fields for the entity and add in the IDs
        entity_fields = eq[entity] << entity.id_field
        entity_fields.uniq!
        1.upto(entity_fields.count).flat_map do |n|
          entity_fields.permutation(n).to_a
        end
      end

      2.upto(graph.entities.length).flat_map do |n|
        entity_choices.permutation(n).map(&:flatten).to_a
      end + entity_choices
    end

    # Get fields which should be included in an index for the given graph
    # @return [Array<Array>]
    def extra_choices(graph, select, eq, range)
      choices = eq.values + range.values << select.to_a

      choices.each do |choice|
        choice.select { |field| graph.entities.include?(field.parent) }
      end

      choices.reject(&:empty?) << []
    end

    def indexes_for_choices(graph, choices, order_choices)
      return [] if choices.size == 0
      Parallel.flat_map(choices, in_processes: Parallel.processor_count / 2) do |index, extra|
        #choices.flat_map do |index, extra|
        indexes = []

        order_choices.each do |order|
          # Append the primary key of the entities in the graph if needed
          order += graph.entities.sort_by(&:name).map(&:id_field) -
              (index + order)

          # Partition into the ordering portion
          indexes += Parallel.flat_map(index.partitions, in_threads: 10) do |index_prefix, order_prefix|
            #indexes += index.partitions.each do |index_prefix, order_prefix|
            hash_fields = index_prefix.take_while do |field|
              field.parent == index.first.parent
            end
            order_fields = index_prefix[hash_fields.length..-1] + \
                           order_prefix + order
            extra_fields = extra - hash_fields - order_fields
            next if order_fields.empty? && extra_fields.empty?

            generated_indexes = []
            if Statement.get_composed_keys(hash_fields + order_fields).nil?
              generated_indexes << generate_index(hash_fields, order_fields, extra_fields, graph)
            else
              generated_indexes << generate_index_with_composite_key_in_place(hash_fields, order_fields, extra_fields, graph)
              generated_indexes << generate_index_with_composite_key_prefix_order(hash_fields, order_fields, extra_fields, graph)
            end
            generated_indexes
          end.flatten(1)
        end

        indexes.compact.uniq
      end.compact.uniq
    end

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, eq, range)
      eq_choices = eq_choices graph, eq
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+)
      range_fields.uniq!
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = extra_choices graph, select, eq, range
      extra_choices = 1.upto(extra_choices.length).flat_map do |n|
        extra_choices.combination(n).map(&:flatten).map(&:uniq)
      end.uniq

      # Generate all possible indices based on the field choices
      choices = eq_choices.product(extra_choices)
      indexes_for_choices graph, choices, order_choices
    end

    def generate_index_with_composite_key_in_place(hash, order, extra, graph)
      composed_keys = Statement.get_composed_keys hash
      _hash = hash.dup
      _hash += composed_keys unless composed_keys.nil?
      _order = []
      order.each do |o|
        _order << o
        next unless o.instance_of?(Fields::IDField)
        next if o.composite_keys.nil?
        _order += o.composite_keys
      end
      generate_index(_hash, _order, extra, graph)
    end

    def generate_index_with_composite_key_prefix_order(hash, order, extra, graph)
      composed_keys = Statement.get_composed_keys hash + order
      order_prefix_composite = order.dup
      order_prefix_composite += composed_keys unless composed_keys.nil?
      generate_index(hash, order_prefix_composite, extra, graph)
    end

    # Generate a new index and ignore if invalid
    # @return [Index]
    def generate_index(hash, order, extra, graph)
      begin
        index = Index.new hash, order.uniq, extra, graph
        @logger.debug { "Enumerated #{index.inspect}" }
      rescue InvalidIndexException
        # This combination of fields is not valid, that's ok
        index = nil
      end

      index
    end
  end
end
