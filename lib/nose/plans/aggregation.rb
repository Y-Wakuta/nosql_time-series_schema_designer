# frozen_string_literal: true

module NoSE
  module Plans
    # A query plan performing a filter without an index
    class AggregationPlanStep < PlanStep
      attr_reader :counts, :sums, :avgs, :maxes, :groupby

      def initialize(counts, sums, avgs, maxes, groupby, state = nil)
        @counts = counts
        @sums = sums
        @avgs = avgs
        @maxes = maxes
        @groupby = groupby
        super()

        return if state.nil?
        @state = state.dup
        update_state
        @state.freeze
      end

      # Two filtering steps are equal if they filter on the same fields
      # @return [Boolean]
      def ==(other)
        other.instance_of?(self.class) && \
          @counts == other.counts && @sums == other.sums && @avgs == other.avgs && \
          @maxes == other.maxes && @groupby == other.groupby
      end

      def hash
        [@counts, @sums, @avgs, @maxes, @groupby].hash
      end

      # :nocov:
      def to_color
        "#{super} COUNT(#{@counts.to_color}) SUM(#{@sums.to_color}) AVG(#{@avgs.to_color}) MAX(#{@maxes.to_color}) GROUP BY(#{@groupby.to_color})" +
          begin
            "#{@parent.state.cardinality} " \
              "-> #{state.cardinality}"
          rescue NoMethodError
            ''
          end
      end
      # :nocov:

      def self.apply(parent, state)

        # check all of query process except aggregation and ordering are done already
        return nil unless state.answered? check_aggregate: false, check_orderby: false

        return nil unless required_fields? parent.fields, state
        return nil if any_parent_is_sort_step? parent
        return nil if any_parent_does_sort? parent

        if any_parent_does_aggregation? parent
          return nil if state.groupby.empty?

          parent_aggs = parent_aggregations(parent)
          # if the parent step does aggregation on db, AggregationStep is still required for aggregation on client
          return AggregationPlanStep.new parent_aggs[:counts], parent_aggs[:sums], parent_aggs[:avgs], parent_aggs[:maxes], state.groupby, state
        end
        AggregationPlanStep.new state.counts, state.sums, state.avgs, state.maxes, state.groupby, state
      end

      # As the same as SQL procedure, sort should be done after Aggregation
      def self.any_parent_is_sort_step?(parent)
        return false if parent.is_a? Plans::RootPlanStep
        return true if parent.instance_of?(Plans::SortPlanStep)
        return any_parent_is_sort_step?(parent.parent)
      end

      def self.any_parent_does_sort?(parent)
        return false if parent.is_a? Plans::RootPlanStep
        return true if parent.instance_of?(Plans::IndexLookupPlanStep) && !parent.order_by.empty?
        return any_parent_does_sort?(parent.parent)
      end

      # aggregation should be done at IndexLookupStep or AggregationPlanStep.
      # Not at both of these steps
      def self.any_parent_does_aggregation?(parent)
        return false if parent.is_a? Plans::RootPlanStep
        return true if parent.instance_of?(Plans::IndexLookupPlanStep) && parent.index.has_aggregation_fields?
        return any_parent_does_aggregation?(parent.parent)
      end

      def self.parent_aggregations(parent)
        return nil if parent.is_a? Plans::RootPlanStep
        if parent.instance_of?(Plans::IndexLookupPlanStep) && parent.index.has_aggregation_fields?
          return {counts: parent.index.count_fields || Set.new, sums: parent.index.sum_fields || Set.new,
                  avgs: parent.index.avg_fields || Set.new,
                  maxes: parent.index.max_fields || Set.new, groupby: parent.index.groupby_fields || Set.new}
        end
        return any_parent_does_aggregation?(parent.parent)
      end

      # Check that we have all the fields we are filtering
      # @return [Boolean]
      def self.required_fields?(fetched_fields, state)
        all_aggregation_fields = Set.new [state.counts, state.sums, state.avgs, state.maxes, state.groupby].map(&:to_a).reject(&:empty?).flatten
        return false if all_aggregation_fields.empty?

        fetched_fields >= all_aggregation_fields
      end
      private_class_method :required_fields?

      def aggregation_fields
        @counts + @sums + @avgs + @maxes + @groupby
      end

      private

      # Apply the filters and perform a uniform estimate on the cardinality
      # @return [void]
      def update_state
        @state.counts -= @counts
        @state.sums -= @sums
        @state.avgs -= @avgs
        @state.maxes -= @maxes
        @state.groupby -= @groupby
        @state.cardinality = 1
      end
    end
  end
end
