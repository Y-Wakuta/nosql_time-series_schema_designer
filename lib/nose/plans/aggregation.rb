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
        return nil unless state.answered? check_aggregate: false
        return nil unless required_fields? parent.fields, state
        AggregationPlanStep.new state.counts, state.sums, state.avgs, state.maxes, state.groupby, state
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
