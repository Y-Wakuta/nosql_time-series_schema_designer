# frozen_string_literal: true

module NoSE
  module Cost
    # A cost model which estimates the number of requests to the backend
    class CassandraIoCost < CassandraCost
      include Subtype

      # Rough cost estimate as the number of requests made
      # @return [Numeric]
      def index_lookup_cost(step)
        return nil if step.state.nil?
        rows = step.state.cardinality
        parts = step.state.hash_cardinality
        fields_size = step.required_select_fields.sum_by(&:size)
        @options[:index_cost_io] + parts * @options[:partition_cost_io] +
          rows * fields_size * @options[:row_cost_io]
      end
    end
  end
end
