# frozen_string_literal: true

module NoSE
  module Plans
    # A query plan performing a filter without an index
    class FilterPlanStep < PlanStep
      attr_reader :eq, :ranges

      def initialize(eq, ranges, state = nil)
        @eq = eq
        @ranges = ranges
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
          @eq == other.eq && @ranges == other.ranges
      end

      def hash
        [@eq.map(&:id), @ranges.empty? ? nil : @ranges.map(&:id)].hash
      end

      # :nocov:
      def to_color
        "#{super} #{@eq.to_color} #{@ranges.to_color} " +
          begin
            "#{@parent.state.cardinality} " \
              "-> #{state.cardinality}"
          rescue NoMethodError
            ''
          end
      end
      # :nocov:

      # Check if filtering can be done (we have all the necessary fields)
      def self.apply(parent, state)
        # Get fields and check for possible filtering
        filter_fields, eq_filter, range_filter = filter_fields parent, state
        return nil if filter_fields.empty?
        return nil if any_parent_does_aggregation? parent

        FilterPlanStep.new eq_filter, range_filter, state \
          if required_fields?(filter_fields, parent)
      end

      # Filtering should be done before IndexLookupStep with aggregations.
      def self.any_parent_does_aggregation?(parent)
        return false if parent.is_a? Plans::RootPlanStep
        return true if parent.instance_of?(Plans::IndexLookupPlanStep) && parent.index.has_aggregation_fields?
        return any_parent_does_aggregation?(parent.parent)
      end


      # Get the fields we can possibly filter on
      def self.filter_fields(parent, state)
        eq_filter = state.eq.select { |field| parent.fields.include? field }
        filter_fields = eq_filter.dup

        if state.ranges.size > 0 && parent.fields >= state.ranges.to_set
          range_filter = state.ranges
          filter_fields += range_filter
        else
          range_filter = []
        end

        [filter_fields, eq_filter, range_filter]
      end
      private_class_method :filter_fields

      # Check that we have all the fields we are filtering
      # @return [Boolean]
      def self.required_fields?(filter_fields, parent)
        filter_fields.map do |field|
          next true if parent.fields.member? field

          # We can also filter if we have a foreign key
          # XXX for now we assume this value is the same
          next unless field.is_a? IDField
          parent.fields.any? do |pfield|
            pfield.is_a?(ForeignKeyField) && pfield.entity == field.parent
          end
        end.all?
      end
      private_class_method :required_fields?

      private

      # Apply the filters and perform a uniform estimate on the cardinality
      # @return [void]
      def update_state
        @state.eq -= @eq
        @state.cardinality *= @eq.map { |field| 1.0 / field.cardinality } \
                                 .inject(1.0, &:*)
        return if @ranges.empty?

        range_selectivity = 0.1
        @state.ranges -= @ranges
        @state.cardinality *= (range_selectivity ** @ranges.count)
      end
    end
  end
end
