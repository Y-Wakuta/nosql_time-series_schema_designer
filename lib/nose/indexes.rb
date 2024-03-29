# frozen_string_literal: true

module NoSE
  # A representation of materialized views over fields in an entity
  class Index
    attr_reader :hash_fields, :order_fields, :extra, :all_fields, :path,
                :entries, :entry_size, :size, :hash_count, :per_hash_count, :graph,
                :count_fields, :sum_fields, :max_fields, :avg_fields, :groupby_fields, :extra_groupby_fields
    attr_accessor :normalized_size

    def initialize(hash_fields, order_fields, extra, graph, count_fields: Set.new, sum_fields: Set.new, max_fields: Set.new, avg_fields: Set.new, groupby_fields: Set.new, extra_groupby_fields: Set.new, saved_key: nil)
      order_set = order_fields.to_set
      @hash_fields = hash_fields.to_set
      @order_fields = order_fields.delete_if { |e| hash_fields.include? e }
      @extra = extra.to_set.delete_if do |e|
        @hash_fields.include?(e) || order_set.include?(e)
      end
      @all_fields = Set.new(@hash_fields).merge(order_set).merge(@extra)

      @count_fields = count_fields
      @sum_fields = sum_fields
      @avg_fields = avg_fields
      @max_fields = max_fields

      @extra_groupby_fields = extra_groupby_fields
      @groupby_fields = groupby_fields + extra_groupby_fields

      validate_hash_fields
      validate_aggregation_fields

      # Store whether this index is an identity
      @identity = @hash_fields == [
        @hash_fields.first.parent.id_field
      ].to_set && graph.nodes.size == 1

      @graph = graph
      @path = graph.longest_path
      @path = nil unless @path.length == graph.size

      validate_graph

      build_hash saved_key
    end

    # Check if this index maps from the primary key to fields from one entity
    # @return [Boolean]
    def identity?
      @identity
    end

    # A simple key which uniquely identifies the index
    # @return [String]
    def key
      @key ||= "i#{Zlib.crc32 hash_str}"
    end

    def key=(key_suffix)
      @key += "_" + key_suffix
    end

    def key_fields
      @hash_fields + @order_fields
    end

    # Look up a field in the index based on its ID
    # @return [Fields::Field]
    def [](field_id)
      @all_fields.find { |field| field.id == field_id }
    end

    # Check if this index is an ID graph
    # @return [Boolean]
    def id_graph?
      @hash_fields.all?(&:primary_key?) && @order_fields.all?(&:primary_key)
    end

    # Produce an index with the same fields but keyed by entities in the graph
    def to_id_graph
      return self if id_graph?

      all_ids = (@hash_fields.to_a + @order_fields + @extra.to_a)
      all_ids.map! { |f| f.parent.id_field }.uniq!

      hash_fields = [all_ids.first]
      order_fields = all_ids[1..-1]
      extra = @all_fields - hash_fields - order_fields

      Index.new hash_fields, order_fields, extra, @graph
    end

    # :nocov:
    def to_color
      fields = [@hash_fields, @order_fields, @extra, @count_fields, @sum_fields, @max_fields, @avg_fields, @groupby_fields].map do |field_group|
        '[' + field_group.map(&:inspect).join(', ') + ']'
      end

      "[magenta]#{key}[/] #{fields[0]} #{fields[1]} → #{fields[2]} aggregate: {c: #{fields[3]}, s: #{fields[4]}, m: #{fields[5]}, a: #{fields[6]}, g: #{fields[7]}} " \
        " [yellow]$#{size}[/]" \
        " [magenta]#{@graph.inspect}[/]"
    end
    # :nocov:

    # Two indices are equal if they contain the same fields
    # @return [Boolean]
    def ==(other)
      hash == other.hash
    end
    alias eql? ==

    # Hash based on the fields, their keys, and the graph
    # @return [String]
    def hash_str
      @hash_str ||= [
        @hash_fields.map(&:id).sort!,
        @order_fields.map(&:id),
        @extra.map(&:id).sort!,
        @graph.unique_edges.map(&:canonical_params).sort!,
        [
          @count_fields&.map(&:id)&.sort! || [],
          @sum_fields&.map(&:id)&.sort! || [],
          @max_fields&.map(&:id)&.sort! || [],
          @avg_fields&.map(&:id)&.sort! || [],
          @groupby_fields&.map(&:id)&.sort! || []
        ]
      ].to_s.freeze
    end

    def hash
      @hash ||= Zlib.crc32 hash_str
    end

    # Check if the index contains a given field
    # @return [Boolean]
    def contains_field?(field)
      @all_fields.include? field
    end

    def has_aggregation_fields?
      has_select_aggregation_fields? || !@groupby_fields.empty?
    end

    def has_select_aggregation_fields?
      [@count_fields, @sum_fields, @max_fields, @avg_fields].any?{|af| not af.empty?}
    end

    private

    # Initialize the hash function and freeze ourselves
    # @return [void]
    def build_hash(saved_key)
      @key = saved_key

      hash
      key
      calculate_size
      freeze
    end

    # Check for valid hash fields in an index
    # @return [void]
    def validate_hash_fields
      fail InvalidIndexException, 'hash fields cannot be empty' \
        if @hash_fields.empty?

      fail InvalidIndexException, 'hash fields can only involve one entity' \
        if @hash_fields.map(&:parent).to_set.size > 1
    end

    def validate_aggregation_fields
      fail InvalidIndexException, 'COUNT, SUM, AVG must be Set' \
        if [@count_fields, @sum_fields, @max_fields, @avg_fields].any?{|af| not af.is_a? Set}
      fail InvalidIndexException, 'COUNT fields need to be exist in index fields' \
        unless @all_fields >= @count_fields
      fail InvalidIndexException, 'SUM fields need to be exist in index fields' \
        unless @all_fields >= @sum_fields
      fail InvalidIndexException, 'AVG fields need to be exist in index fields' \
        unless @all_fields >= @avg_fields
      fail InvalidIndexException, 'GROUP BY fields should be exist in key fields' \
        unless @groupby_fields.empty? || (@hash_fields + @order_fields) >= @groupby_fields
    end

    # Ensure an index is nonempty
    # @return [void]
    def validate_nonempty
      fail InvalidIndexException, 'must have fields other than hash fields' \
        if @order_fields.empty? && @extra.empty?
    end

    # Ensure an index and its fields correspond to a valid graph
    # @return [void]
    def validate_graph
      validate_graph_entities
      validate_graph_keys
    end

    # Ensure the graph of the index is valid
    # @return [void]
    def validate_graph_entities
      entities = @all_fields.map(&:parent).to_set
      fail InvalidIndexException, 'graph entities do match index' \
        unless entities == @graph.entities.to_set
    end

    # We must have the primary keys of the all entities in the graph
    # @return [void]
    def validate_graph_keys
      fail InvalidIndexException, 'missing graph entity keys' \
        unless @graph.entities.map(&:id_field).all? do |field|
        @hash_fields.include?(field) || @order_fields.include?(field)
      end
    end

    # Precalculate the size of the index
    # @return [void]
    def calculate_size
      @hash_count = @hash_fields.product_by(&:cardinality)

      # XXX This only works if foreign keys span all possible keys
      #     Take the maximum possible count at each join and multiply
      @entries = @graph.entities.map(&:count).max
      @per_hash_count = (@entries * 1.0 / @hash_count)

      @entry_size = @all_fields.sum_by(&:size)
      @size = @entries * @entry_size
    end
  end

  class EachTimeStepIndexes
    attr_accessor :indexes

    def initialize(indexes)
      @indexes = indexes
    end
  end

  class TimeDependIndexes
    attr_accessor :indexes_all_timestep

    def initialize(indexes_all_timestep)
      @indexes_all_timestep = indexes_all_timestep
    end
  end

  # Thrown when something tries to create an invalid index
  class InvalidIndexException < StandardError
  end

  # Allow entities to create their own indices
  class Entity
    # Create a simple index which maps entity keys to other fields
    # @return [Index]
    def simple_index
      Index.new [id_field], [], fields.values - [id_field],
                QueryGraph::Graph.from_path([id_field]), saved_key: name
    end
  end

  # Allow statements to materialize views
  class Statement
    # Construct an index which acts as a materialized view for a query
    # @return [Index]
    def materialize_view
      eq = materialized_view_eq join_order.first
      order_fields = materialized_view_order(join_order.first) - eq

      # add composite key to prefix of order fields.
      composed_keys = Statement.get_composed_keys eq + order_fields
      order_fields += composed_keys unless composed_keys.nil?

      Index.new(eq, order_fields,
                all_fields - (@eq_fields + @order).to_set, graph)
    end

    def materialize_view_with_aggregation
      eq = materialized_view_eq join_order.first

      # GROUP BY on
      if eq.to_set > @groupby.to_set
        groupby_in_eq = eq.to_set & @groupby.to_set
        eq = eq.delete_if { |e| groupby_in_eq.include? e}
      end
      order_fields = materialized_view_order(join_order.first) - eq
      order_fields.uniq!

      extra_groupby = []
      unless (order_fields.to_set & @groupby.to_set).empty?
          last_index_of_groupby_field = @groupby.map{|g| order_fields.include?(g) ? order_fields.index(g) : -1}.max
          extra_groupby = order_fields[0..last_index_of_groupby_field]
      end

      # add composite key to prefix of order fields.
      composed_keys = Statement.get_composed_keys eq + order_fields
      order_fields += composed_keys unless composed_keys.nil?
      Index.new(eq, order_fields, all_fields - (@eq_fields + @order).to_set, graph, count_fields: @counts, sum_fields: @sums,
                      max_fields: @maxes, avg_fields: @avgs, groupby_fields: @groupby, extra_groupby_fields: extra_groupby)
    end

    def self.get_composed_keys(key_fields)
       composite_key_fields = key_fields
                               .select{|i| i.instance_of?(Fields::IDField) && !i.composite_keys.nil?}
                               .flat_map(&:composite_keys)
       composite_key_fields unless key_fields.to_set >= composite_key_fields.to_set
    end

    private

    # Get the fields used as parition keys for a materialized view
    # based over a given entity
    # @return [Array<Fields::Field>]
    def materialized_view_eq(hash_entity)
      eq = @eq_fields.select { |field| field.parent == hash_entity }
      eq = [@eq_fields.sort_by{|ef| ef.cardinality}.last] if eq.empty?

      eq
    end

    # Get the ordered keys for a materialized view
    # @return [Array<Fields::Field>]
    def materialized_view_order(hash_entity)
      # Start the ordered fields with the equality predicates
      # on other entities, followed by all of the attributes
      # used in ordering, then the range field
      order_fields = @eq_fields.select do |field|
        field.parent != hash_entity
      end
      order_fields += @range_fields.sort_by{|rf| @groupby.include?(rf) ? 0 : 1}
      order_fields += @groupby.select{|g| not order_fields.include? g}
      order_fields += @order.select{|g| not order_fields.include? g}

      # Ensure we include IDs of the final entity
      order_fields += join_order.map(&:id_field)

      order_fields.uniq
    end
  end
end
