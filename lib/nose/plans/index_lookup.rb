# frozen_string_literal: true

module NoSE
  module Plans
    # Superclass for steps using indices
    class IndexLookupPlanStep < PlanStep
      extend Forwardable

      attr_reader :index, :eq_filter, :range_filter, :limit, :order_by, :fields
      delegate hash: :index

      def initialize(index, state = nil, parent = nil)
        super()
        @index = index

        if state && state.query
          all_fields = state.query.all_fields
          @fields = (@index.hash_fields + @index.order_fields).to_set + \
                    (@index.extra.to_set & all_fields)
        else
          @fields = @index.all_fields
        end

        return if state.nil?
        @state = state.dup
        update_state parent
        @state.freeze
      end

      # :nocov:
      def to_color
        if @state.nil?
          "#{super} #{@index.to_color}"
        else
          "#{super} #{@index.to_color} * " \
            "#{@state.cardinality}/#{@state.hash_cardinality} eq_filter: {#{@eq_filter}}, order by: #{@order_by} "
        end
      end
      # :nocov:

      # Two index steps are equal if they use the same index
      def ==(other)
        other.instance_of?(self.class) && @index == other.index
      end
      alias eql? ==

      # Check if this step can be applied for the given index,
      # returning a possible application of the step
      # @return [IndexLookupPlanStep]
      def self.apply(parent, index, state, check_aggregation: true)
        # Validate several conditions which identify if this index is usable
        begin
          validate_step_state state, parent, index, check_aggregation
        rescue InvalidIndex
          return nil
        end

        IndexLookupPlanStep.new(index, state, parent)
      end

      def self.validate_step_state(state, parent, index, check_aggregation)
        check_joins index, state
        check_forward_lookup parent, index, state
        check_parent_index parent, index, state
        check_all_hash_fields parent, index, state
        check_graph_fields parent, index, state
        check_last_fields index, state
        if check_aggregation
          check_has_only_required_aggregations index, state
          check_parent_groupby parent
          check_parent_aggregation parent
        end
      end

      def self.check_has_only_required_aggregations(index, state)
        fail InvalidIndex if state.groupby.empty? and not index.groupby_fields.empty?
        fail InvalidIndex if (not state.groupby.empty? and not index.groupby_fields.empty?) and not state.groupby <= index.groupby_fields
      end

      def self.check_parent_groupby(parent)
        return unless parent.is_a? Plans::IndexLookupPlanStep
        # no column family with GROUP BY can become parent of any index
        fail InvalidIndex if parent.index.has_aggregation_fields?
      end

      # remove invalid query plans because of aggregation fields
      def self.check_aggregate_fields(parent, index)
        return unless parent.is_a? IndexLookupPlanStep

        # fields used for join parent_index and current index
        join_fields = parent.index.all_fields & (index.hash_fields + index.order_fields)
        aggregation_fields = parent.index.count_fields + parent.index.sum_fields + parent.index.max_fields + parent.index.avg_fields

        # if all primary fields are used for joining column families are aggregate fields, raise InvalidIndex
        fail InvalidIndex if join_fields.select(&:primary_key).all?{|f| aggregation_fields.include? f}
      end

      def self.check_parent_aggregation(parent)
        fail InvalidIndex if parent.is_a? Plans::AggregationPlanStep
      end
      private_class_method :check_parent_aggregation

      # Check that this index is a valid continuation of the set of joins
      # @raise [InvalidIndex]
      # @return [void]
      def self.check_joins(index, state)
        fail InvalidIndex \
          unless index.graph.entities.include?(state.joins.first) &&
          (index.graph.unique_edges &
            state.graph.unique_edges ==
            index.graph.unique_edges)
      end
      private_class_method :check_joins

      # Check that this index moves forward on the list of joins
      # @raise [InvalidIndex]
      # @return [void]
      def self.check_forward_lookup(parent, index, state)
        # XXX This disallows plans which look up additional attributes
        #     for entities other than the final one
        fail InvalidIndex if index.graph.size == 1 && state.graph.size > 1 &&
          !parent.is_a?(RootPlanStep)
        fail InvalidIndex if index.identity? && state.graph.size > 1
      end
      private_class_method :check_forward_lookup

      # Check if this index can be used after the current parent
      # @return [Boolean]
      def self.invalid_parent_index?(state, index, parent_index)
        return false if parent_index.nil?

        # We don't do multiple lookups by ID for the same entity set
        return true if parent_index.identity? &&
                       index.graph == parent_index.graph

        last_parent_entity = state.joins.reverse.find do |entity|
          parent_index.graph.entities.include? entity
        end
        parent_ids = Set.new [last_parent_entity.id_field]
        parent_ids += parent_ids.flat_map(&:composite_keys).compact
        has_ids = parent_ids.subset? parent_index.all_fields

        hash_order_prefix = index.hash_fields + index.order_fields.take((parent_ids - index.hash_fields).size)
        # If the last step gave an ID, we must use it
        # XXX This doesn't cover all cases
        return true if has_ids && hash_order_prefix.to_set != parent_ids

        # If we're looking up from a previous step, only allow lookup by ID
        return true unless (index.graph.size == 1 &&
          parent_index.graph != index.graph) ||
          hash_order_prefix == parent_ids

        return true if is_useless_parent?(state, index, parent_index)

        return true unless is_both_have_composite_key?(state, index, parent_index)

        false
      end
      private_class_method :invalid_parent_index?

      # Check that this index is a valid continuation of the set of joins
      # @raise [InvalidIndex]
      # @return [void]
      def self.check_parent_index(parent, index, state)
        fail InvalidIndex \
          if invalid_parent_index? state, index, parent.parent_index
      end
      private_class_method :check_parent_index

      # If one column family used as materialized view and second step of query plan,
      # costs of each column family differs because of the cost of second step changes according to the cardinality of the first step.
      # Validation process fails because of two different cost of the same column family.
      # This happens when one query has equality condition of id field and non-id field.
      def self.is_useless_parent?(state,index,parent_index)

        #SELECT * FROM entity WHERE A = ? AND B = ?
        # parent: [A][B]->[C,D]
        # index:  [B][A]->[C,D,E]
        return true if index.extra >= parent_index.extra and \
                          state.query.eq_fields >= (parent_index.hash_fields + parent_index.order_fields.to_set) and \
                          parent_index.hash_fields == index.order_fields.to_set and \
                          parent_index.order_fields.to_set == index.hash_fields

        #SELECT * FROM entity WHERE A = ? AND B = ?
        # parent: [A][B]->[C,D]
        # index:  [A,B][F]->[C,D,E]
        return true if index.hash_fields >= state.query.eq_fields and \
                       index.all_fields >= parent_index.all_fields

        #SELECT E FROM entity WHERE A = ? AND B = ?
        # parent: [A,B][]->[C,D] or [A][B]->[C,D]
        # index:  [A][B]->[E]
        return true if state.query.eq_fields >= index.hash_fields and \
                      (index.hash_fields + index.order_fields.to_set) >= state.query.eq_fields and \
                      index.all_fields >= state.fields

        false
      end

      def self.is_both_have_composite_key?(state, index, parent_index)
        overlapped_key_fields = (index.key_fields & parent_index.all_fields).select{|kf| kf.primary_key?}.to_set
        current_composite_keys = overlapped_key_fields.flat_map(&:composite_keys).compact.to_set
        return true if current_composite_keys.empty?
        return false unless parent_index.key_fields.to_set >= current_composite_keys

        join_keys = overlapped_key_fields + current_composite_keys
        return true if index.hash_fields.to_set >= current_composite_keys or \
                            index.order_fields.drop_while{|of| state.eq.include? of}
                                 .take((join_keys - index.hash_fields - state.eq).size)
                                 .to_set == (join_keys - index.hash_fields - state.eq).to_set

        false
      end

      # Check that we have all hash fields needed to perform the lookup
      # @raise [InvalidIndex]
      # @return [void]
      def self.check_all_hash_fields(parent, index, state)
        fail InvalidIndex unless index.hash_fields.all? do |field|
          (parent.fields + state.given_fields).include? field
        end
      end
      private_class_method :check_all_hash_fields

      # Get fields in the query relevant to this index
      # and check that they are provided for us here
      # @raise [InvalidIndex]
      # @return [void]
      def self.check_graph_fields(parent, index, state)
        hash_entity = index.hash_fields.first.parent
        graph_fields = state.fields_for_graph(index.graph, hash_entity).to_set
        graph_fields -= parent.fields # exclude fields already fetched
        fail InvalidIndex unless graph_fields.subset?(index.all_fields)
      end
      private_class_method :check_graph_fields

      # Check that we have the required fields to move on with the next lookup
      # @return [Boolean]
      def self.last_fields?(index, state)
        index_includes = lambda do |fields|
          fields.all? { |f| index.all_fields.include? f }
        end

        # We must have either the ID or all the fields
        # for leaf entities in the original graph
        leaf_entities = index.graph.entities.select do |entity|
          state.graph.leaf_entity?(entity)
        end
        leaf_entities.all? do |entity|
          index_includes.call([entity.id_field]) ||
            index_includes.call(state.fields.select { |f| f.parent == entity })
        end
      end
      private_class_method :last_fields?

      # @raise [InvalidIndex]
      # @return [void]
      def self.check_last_fields(index, state)
        fail InvalidIndex unless last_fields?(index, state)
      end
      private_class_method :check_last_fields

      private

      # Get the set of fields which can be filtered by the ordered keys
      # @return [Array<Fields::Field>]
      def range_order_prefix
        order_prefix = (@state.eq - @index.hash_fields) & @index.order_fields
        order_prefix << @state.range unless @state.range.nil?
        order_prefix = order_prefix.zip(@index.order_fields)
        order_prefix.take_while { |x, y| x == y }.map(&:first)
      end

      # Perform any ordering implicit to this index
      # @return [Boolean] whether this index is by ID
      def resolve_order(indexed_by_id)
        unless is_order_appliable_for_state?
          @order_by = []
          return
        end

        # We can't resolve ordering if we're doing an ID lookup
        # since only one record exists per row (if it's the same entity)
        # We also need to have the fields used in order
        order_prefix = @state.order_by.longest_common_prefix(
          @index.order_fields - @eq_filter.to_a
        )
        if indexed_by_id && order_prefix.map(&:parent).to_set ==
          Set.new([@index.hash_fields.first.parent])
          order_prefix = []
        else
          @state.order_by -= order_prefix
        end
        @order_by = order_prefix
      end

      def is_indexed_by_id?
         first_join = @state.query.join_order.detect do |entity|
          @index.graph.entities.include? entity
        end
        @index.hash_fields.include?(first_join.id_field)
      end

      def is_order_appliable_for_state?
        # if the query does not have any aggregation, order by can be applied
        return true unless @state.query.has_aggregation_fields?

        # if the aggregation is done in this IndexLookup, order can be applied
        return true if @index.has_aggregation_fields?

        # if the aggregation is done on client, order by should be done after the aggregation.
        # Thus, the ordering cannot be applied in this step
        false
      end

      # Strip the graph for this index, but if we haven't fetched all
      # fields, leave the last one so we can perform a separate ID lookup
      # @return [void]
      def strip_graph
        hash_entity = @index.hash_fields.first.parent
        @state.graph = @state.graph.dup
        required_fields = @state.fields_for_graph(@index.graph, hash_entity,
                                                  select: true).to_set
        if required_fields.subset?(@index.all_fields) &&
          @state.graph == @index.graph
          removed_nodes = @state.joins[0..@index.graph.size]
          @state.joins = @state.joins[@index.graph.size..-1]
        else
          removed_nodes = if index.graph.size == 1
                            []
                          else
                            @state.joins[0..@index.graph.size - 2]
                          end
          @state.joins = @state.joins[@index.graph.size - 1..-1]
        end

        # Remove nodes which have been processed from the graph
        @state.graph.remove_nodes removed_nodes
      end

      # Update the cardinality of this step, applying a limit if possible
      def update_cardinality(parent, indexed_by_id)
        # Calculate the new cardinality assuming no limit
        # Hash cardinality starts at 1 or is the previous cardinality
        if parent.is_a?(RootPlanStep)
          @state.hash_cardinality = 1
        else
          @state.hash_cardinality = parent.state.cardinality
        end

        # Filter the total number of rows by filtering on non-hash fields
        cardinality = @index.per_hash_count * @state.hash_cardinality

        # cassandra requires all prefix order_fields to be specified to use fields in order_fields
        eq_fields = @eq_filter - @index.hash_fields

        @state.cardinality = Cardinality.filter cardinality,
                                                eq_fields,
                                                @range_filter

        # Check if we can apply the limit from the query
        # This occurs either when we are on the first or last index lookup
        # and the ordering of the query has already been resolved
        order_resolved = @state.order_by.empty? && @state.graph.size == 1
        return unless (@state.answered?(check_limit: false) ||
          parent.is_a?(RootPlanStep) && order_resolved) &&
          !@state.query.limit.nil?

        # XXX Assume that everything is limited by the limit value
        #     which should be fine if the limit is small enough
        @limit = @state.query.limit
        if parent.is_a?(RootPlanStep)
          @state.cardinality = [@limit, @state.cardinality].min
          @state.hash_cardinality = 1
        else
          @limit = @state.cardinality = @state.query.limit

          # If this is a final lookup by ID, go with the limit
          if @index.graph.size == 1 && indexed_by_id
            @state.hash_cardinality = @limit
          else
            @state.hash_cardinality = parent.state.cardinality
          end
        end
      end

      # Modify the state to reflect the fields looked up by the index
      # @return [void]
      def update_state(parent)
        order_prefix = range_order_prefix.to_set

        # Find fields which are filtered by the index
        @eq_filter = @index.hash_fields + (@state.eq & order_prefix)

        # composite key should be added only if the keys are used for join.
        # Therefore, the first step of the plan does not have to have composite keys to its eq_filter
        @eq_filter += @eq_filter.select(&:primary_key?).flat_map(&:composite_keys).compact unless parent.instance_of?(Plans::RootPlanStep)
        if order_prefix.include?(@state.range) \
          or @index.hash_fields.include?(@state.range) # range_filter also could be adapted to hash_fields
          @range_filter = @state.range
          @state.range = nil
        else
          @range_filter = nil
        end

        # Remove fields resolved by this index
        @state.fields -= @index.all_fields
        @state.eq -= @eq_filter
        @state.counts -= @index.count_fields.to_set
        @state.sums -= @index.sum_fields
        @state.maxes -= @index.max_fields
        @state.avgs -= @index.avg_fields
        if @state.groupby == @index.groupby_fields
          # groupby fields can be resolved with only required group by
          @state.groupby -= @index.groupby_fields
        end

        indexed_by_id = is_indexed_by_id?
        resolve_order(indexed_by_id)
        strip_graph
        update_cardinality parent, indexed_by_id
      end
    end

    class ExtractPlanStep < IndexLookupPlanStep
      # Check if this step can be applied for the given index,
      # returning a possible application of the step
      # @return [IndexLookupPlanStep]
      def self.apply(parent, index, state, check_aggregation: true)
        # Validate several conditions which identify if this index is usable
        begin
          validate_step_state state, parent, index, check_aggregation
        rescue InvalidIndex
          return nil
        end

        ExtractPlanStep.new(index, state, parent)
      end
    end

    class InvalidIndex < StandardError
    end
  end
end
