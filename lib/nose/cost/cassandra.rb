# frozen_string_literal: true

module NoSE
  module Cost
    # A cost model which estimates the number of requests to the backend
    class CassandraCost < Cost
      include Subtype

      # Rough cost estimate as the number of requests made
      # @return [Numeric]
      def index_lookup_cost(step)
        return nil if step.state.nil?
        rows = step.state.cardinality
        parts = step.state.hash_cardinality

        @options[:index_cost] + parts * @options[:partition_cost] +
          rows * @options[:row_cost]
      end

      # Cost estimate as number of entities deleted
      def delete_cost(step)
        return nil if step.state.nil?
        step.state.cardinality * @options[:delete_cost]
      end

      # Cost estimate as number of entities inserted
      def insert_cost(step)
        return nil if step.state.nil?
        step.state.cardinality * @options[:insert_cost]
      end

      # The cost of aggregating the intermediate results at the last step
      # @return [Fixnum]
      def aggregation_cost(_step)
        # execute each aggregation for each field separately
        _step.aggregation_fields.size * _step.state.hash_cardinality * @options[:aggregation_cost]
      end
    end
  end
end
