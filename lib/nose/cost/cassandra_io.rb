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

        #if rows > 5_000
        #  #puts "slow query cost model"
        #  index_lookup_high_latency fields_size, parts, rows
        #else
          #puts "fast query cost model"
        #puts "rows: #{rows}, parts: #{parts}, fields_size: #{fields_size}"
        estimated_cost = index_lookup_low_latency fields_size, parts, rows
        estimated_cost
      end

      private

      def index_lookup_low_latency(fields_size, parts, rows)
        #@options[:index_cost_low_io] + parts * @options[:partition_cost_low_io] +
        #  rows * parts * fields_size * @options[:row_cost_low_io]
        @options[:index_cost_low_io] + parts * @options[:partition_cost_low_io] +
          rows * fields_size * @options[:row_cost_low_io]
      end

      def index_lookup_high_latency(fields_size, parts, rows)
        #@options[:index_cost_high_io] + parts * @options[:partition_cost_high_io] +
        #  rows * parts * fields_size * @options[:row_cost_high_io]
        @options[:index_cost_high_io] + parts * @options[:partition_cost_high_io] +
          rows * fields_size * @options[:row_cost_high_io]
      end
    end
  end
end
