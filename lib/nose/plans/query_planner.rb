# frozen_string_literal: true

require 'forwardable'
require 'ostruct'

module NoSE
  module Plans
    # Ongoing state of a query throughout the execution plan
    class QueryState
      attr_accessor :fields, :eq, :ranges, :order_by, :graph,
                    :joins, :cardinality, :hash_cardinality, :given_fields, :counts, :sums, :avgs, :maxes, :groupby
      attr_reader :query, :model

      def initialize(query, model)
        @query = query
        @model = model
        @fields = query.select
        @eq = query.eq_fields.dup
        @ranges = query.range_fields
        @graph = query.graph
        @joins = query.materialize_view.graph.join_order(@eq)
        @order_by = query.order.dup
        @counts = query.counts
        @sums = query.sums
        @avgs = query.avgs
        @maxes = query.maxes
        @groupby = query.groupby || Set.new

        # We never need to order by fields listed in equality predicates
        # since we'll only ever have rows with a single value
        @order_by -= @eq.to_a

        @cardinality = 1 # this will be updated on the first index lookup
        @hash_cardinality = 1
        @given_fields = @eq.dup
      end

      # All the fields referenced anywhere in the query
      def all_fields
        all_fields = @fields + @eq
        all_fields += @ranges
        all_fields
      end

      # :nocov:
      def to_color
        @query.text +
          "\n  fields: " + @fields.map(&:to_color).to_a.to_color +
          "\n      eq: " + @eq.map(&:to_color).to_a.to_color +
          "\n   range: " + (@ranges.empty? ? '(empty)' : @ranges.map(&:name).to_color) +
          "\n   order: " + @order_by.map(&:to_color).to_a.to_color +
          "\n    graph: " + @graph.inspect
      end
      # :nocov:

      # Check if the query has been fully answered
      # @return [Boolean]
      def answered?(check_limit: true, check_aggregate: true, check_orderby: true)
        done = @fields.empty? && @eq.empty? && @ranges.empty? &&
               @joins.empty? && @graph.empty?
        done &= @order_by.empty? if check_orderby

        done &= @counts.empty? && @sums.empty? && @maxes.empty? \
              && @avgs.empty? && @groupby.empty? if check_aggregate

        # Check if the limit has been applied
        done &&= @cardinality <= @query.limit unless @query.limit.nil? ||
                                                     !check_limit

        done
      end

      # Get all fields relevant for filtering in the given
      # graph, optionally including selected fields
      # @return [Array<Field>]
      def fields_for_graph(graph, include_entity, select: false)
        graph_fields = @eq + @order_by + @ranges

        # If necessary, include ALL the fields which should be selected,
        # otherwise we can exclude fields from leaf entity sets since
        # we may end up selecting these with a separate index lookup
        entities = graph.entities
        graph_fields += @fields.select do |field|
          entities.include?(field.parent) &&
            (select || !graph.leaf_entity?(field.parent) ||
             (field.parent == include_entity && graph.size > 1))
        end

        graph_fields.select { |field| entities.include? field.parent }
      end
    end

    # A tree of possible query plans
    class QueryPlanTree
      include Enumerable

      attr_reader :root
      attr_accessor :cost_model

      def initialize(state, cost_model)
        @root = RootPlanStep.new(state)
        @cost_model = cost_model
      end

      # Select all plans which use only a given set of indexes
      def select_using_indexes(indexes)
        select do |plan|
          plan.all? do |step|
            !step.is_a?(Plans::IndexLookupPlanStep) ||
              indexes.include?(step.index)
          end
        end
      end

      # The query this tree of plans is generated for
      # @return [Query]
      def query
        @root.state.query
      end

      # Enumerate all plans in the tree
      def each
        nodes = [@root]

        until nodes.empty?
          node = nodes.pop
          if node.children.empty?
            # This is just an extra check to make absolutely
            # sure we never consider invalid statement plans
            fail unless node.state.answered?

            yield node.parent_steps @cost_model
          else
            nodes.concat node.children.to_a
          end
        end
      end

      # Return the total number of plans for this statement
      # @return [Integer]
      def size
        to_a.size
      end

      # :nocov:
      def to_color(step = nil, indent = 0)
        step = @root if step.nil?
        this_step = '  ' * indent + step.to_color
        this_step << " [yellow]$#{step.cost.round 5}[/]" \
          unless step.is_a?(RootPlanStep) || step.cost.nil?
        this_step + "\n" + step.children.map do |child_step|
          to_color child_step, indent + 1
        end.reduce('', &:+)
      end
      # :nocov:
    end

    # Thrown when it is not possible to construct a plan for a statement
    class NoPlanException < StandardError
    end

    # A single plan for a query
    class QueryPlan < AbstractPlan
      attr_reader :steps
      attr_accessor :query, :cost_model

      include Comparable
      include Enumerable

      # Most of the work is delegated to the array
      extend Forwardable
      def_delegators :@steps, :each, :<<, :[], :==, :===, :eql?,
                     :inspect, :to_s, :to_a, :to_ary, :last, :length, :count

      def initialize(query, cost_model)
        @steps = []
        @query = query
        @cost_model = cost_model
      end

      # The weight of this query for a given workload
      # @return [Fixnum]
      def weight
        return 1 if @workload.nil?

        @workload.statement_weights[@query]
      end

      # Groups for plans are stored in the query
      # @return [String]
      def group
        @query.group
      end

      # Name plans after the associated query
      # @return [String]
      def name
        @query.text
      end

      # Fields selected by this plan
      # @return [Array<Fields::Field>]
      def select_fields
        @query.select
      end

      # Parameters to this execution plan
      def params
        @query.conditions
      end

      # Two plans are compared by their execution cost
      # @return [Boolean]
      def <=>(other)
        cost <=> other.cost
      end

      # The estimated cost of executing the query using this plan
      # @return [Numeric]
      def cost
        costs = @steps.map(&:cost)
        costs.inject(0, &:+) unless costs.any?(&:nil?)
      end

      # Get the indexes used by this query plan
      # @return [Array<Index>]
      def indexes
        @steps.select { |step| step.is_a? IndexLookupPlanStep }.map(&:index)
      end
    end

    # A query planner which can construct a tree of query plans
    class QueryPlanner
      def initialize(model, indexes, cost_model)
        @logger = Logging.logger['nose::query_planner']

        @model = model
        @indexes = indexes
        @cost_model = cost_model
      end

      # Find a tree of plans for the given query
      # @return [QueryPlanTree]
      # @raise [NoPlanException]
      def find_plans_for_query(query)
        state = QueryState.new query, @model
        state.freeze
        tree = QueryPlanTree.new state, @cost_model

        indexes_by_joins = indexes_for_query(query, state.joins)
        find_plans_for_step tree.root, indexes_by_joins

        if tree.root.children.empty?
          tree = QueryPlanTree.new state, @cost_model
          find_plans_for_step tree.root, indexes_by_joins, prune: false
          fail NoPlanException, "#{query.class} #{query.inspect} #{tree.inspect}"
        end

        @logger.debug { "Plans for #{query.inspect}: #{tree.inspect}" }
        does_include_materialized_plan tree, query

        validate_query_plans tree if query.instance_of? Query

        tree
      end

      # check the query plan tree include materialized plan
      def does_include_materialized_plan(tree, query)
        materialize_plans = tree.select do |plan|
          plan.indexes.size == 1
        end
        unless materialize_plans.any? {|mvp| mvp.indexes.include? query.materialize_view}
          #fail 'materialized plan is not enumerated'
        end
      end

      # Get the minimum cost plan for executing this query
      # @return [QueryPlan]
      def min_plan(query)
        find_plans_for_query(query).min
      end

      # Remove plans ending with this step in the tree
      # @return[Boolean] true if pruning resulted in an empty tree
      def prune_plan(prune_step)
        # Walk up the tree and remove the branch for the failed plan
        while prune_step.children.length <= 1 &&
            !prune_step.is_a?(RootPlanStep)
          prev_step = prune_step
          prune_step = prune_step.parent
        end

        prune_step.children.delete prev_step

        # If we reached the root, we have no plan
        return true if prune_step.is_a?(RootPlanStep) && prune_step.children.empty?

        false
      end

      private

      def validate_query_plans(tree)
        tree.each do |plan|
          validate_query_plan_has_required_field_for_groupby plan
          validate_query_plan_does_filtering_before_aggregation plan
        end
      end

      def validate_query_plan_has_required_field_for_groupby(plan)
        required_groupby_fields = plan.query.groupby.to_set
        last_aggregatable_step = plan.steps.select{|s| s.is_a?(Plans::IndexLookupPlanStep) || s.is_a?(Plans::AggregationPlanStep)}.last
        if last_aggregatable_step.is_a?(Plans::IndexLookupPlanStep)
          given_groupby_fields = last_aggregatable_step.index.groupby_fields
        elsif last_aggregatable_step.is_a?(Plans::AggregationPlanStep)
          given_groupby_fields = last_aggregatable_step.groupby
        end
        fail "candidate plans has more groupby fields than required: required #{required_groupby_fields.map(&:id)}, given: #{given_groupby_fields.flatten.map(&:id)}, query: #{plan.query.comment}, plan: #{plan.inspect}" if given_groupby_fields > required_groupby_fields
      end

      def validate_query_plan_does_filtering_before_aggregation(plan)
        return unless plan.steps.any?{|s| s.instance_of?(Plans::FilterPlanStep)}
        last_filter_step_idx = 0

        plan.each_with_index do |step, idx|
          if step.instance_of?(Plans::FilterPlanStep)
            last_filter_step_idx = idx
          end
        end

        fail "any filtering should be done before aggregation #{plan.query.comment}: #{plan.inspect}" \
          if plan.steps[0...last_filter_step_idx].any? do |s|
            s.instance_of?(Plans::AggregationPlanStep) || \
                  (s.instance_of?(Plans::IndexLookupPlanStep) && s.index.has_aggregation_fields?)
          end
      end

      # Produce indexes possibly useful for this query
      # grouped by the first entity they join on
      # @return [Hash]
      def indexes_for_query(query, joins)
        indexes_by_joins = Hash.new { |h, k| h[k] = Set.new }
        @indexes.each do |index|
          # Limit indices to those which cross the query path
          next unless index.graph.entities.to_set.subset? \
            query.graph.entities.to_set

          first_entity = joins.find do |entity|
            index.graph.entities.include?(entity)
          end
          indexes_by_joins[first_entity].add index
        end

        indexes_by_joins
      end

      def prepare_next_step(step, child_steps, indexes_by_joins)
        step.children = child_steps
        child_steps.each { |new_step| new_step.calculate_cost @cost_model }
        child_steps.each do |child_step|
          find_plans_for_step child_step, indexes_by_joins

          # Remove this step if finding a plan from here failed
          if child_step.children.empty? && !child_step.state.answered?
            step.children.delete child_step
          end
        end
      end

      # Find possible query plans for a query starting at the given step
      # @return [void]
      def find_plans_for_step(step, indexes_by_joins, prune: true, prune_slow: true)
        return if step.state.answered?

        steps = find_steps_for_state step, step.state, indexes_by_joins

        if !steps.empty?
          prepare_next_step step, steps, indexes_by_joins
        elsif prune
          return if step.is_a?(RootPlanStep) || prune_plan(step.parent)
        else
          step.children = [PrunedPlanStep.new]
        end
      end

      # Find all possible plan steps not using indexes
      # @return [Array<PlanStep>]
      def find_nonindexed_steps(parent, state)
        steps = []
        return steps if parent.is_a? RootPlanStep

        [SortPlanStep, FilterPlanStep, LimitPlanStep, AggregationPlanStep].each \
          { |step| steps.push step.apply(parent, state) }
        steps.flatten!
        steps.compact!

        steps
      end

      def find_indexed_steps(parent, state, indexes_by_joins, step_class, check_aggregation: true)
        steps = []
        # Don't allow indices to be used multiple times
        indexes = (indexes_by_joins[state.joins.first] || Set.new).to_set
        used_indexes = parent.parent_steps.indexes.to_set
        Parallel.map(indexes - used_indexes, in_threads: Parallel.processor_count / 4) do |index|
          new_step = step_class.apply parent, index, state, check_aggregation: check_aggregation
          next if new_step.nil?

          new_step.add_fields_from_index index
          steps.push new_step
        end
        steps
      end

      # Get a list of possible next steps for a query in the given state
      # @return [Array<PlanStep>]
      def find_steps_for_state(parent, state, indexes_by_joins)
        steps = find_nonindexed_steps parent, state
        return steps unless steps.empty?

        steps += find_indexed_steps parent, state, indexes_by_joins, IndexLookupPlanStep
        steps
      end
    end


    class PrunedQueryPlanner < QueryPlanner
      def initialize(model, indexes, cost_model, index_step_size_threshold)
        super(model, indexes, cost_model)
        @index_step_size_threshold = index_step_size_threshold
      end

      # Find possible query plans for a query starting at the given step
      # @return [void]
      def find_plans_for_step(step, indexes_by_joins, prune: true, prune_slow: true)
        return if step.state.answered?

        steps = find_steps_for_state step, step.state, indexes_by_joins

        if prune_slow
          steps.each do |child_step|
            if is_apparently_slow_plan? step, child_step
              prune_plan step
              return
            end
          end
        end

        if !steps.empty?
          prepare_next_step step, steps, indexes_by_joins
        elsif prune
          return if step.is_a?(RootPlanStep) || prune_plan(step.parent)
        else
          step.children = [PrunedPlanStep.new]
        end
      end

      private

      # if the query plan joins many CFs, the query plan would be too slow
      def is_apparently_slow_plan?(step, child_step)
        current = step
        indexPlanStepCount = 0
        while !current.is_a? RootPlanStep
          if current.is_a? IndexLookupPlanStep
            indexPlanStepCount += 1
          end
          current = current.parent
        end
        indexPlanStepCount >= @index_step_size_threshold && child_step.is_a?(IndexLookupPlanStep) # threshold
      end
    end

    class MigrateSupportSimpleQueryPlanner < PrunedQueryPlanner
      def find_steps_for_state(parent, state, indexes_by_joins)
        find_indexed_steps parent, state, indexes_by_joins, ExtractPlanStep, check_aggregation: false
      end
    end
  end
end
