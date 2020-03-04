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

        lookup_cost = @options[:index_cost] + parts * @options[:partition_cost] +
          rows * @options[:row_cost]
        lookup_cost += count_cost step unless step.index.count_fields.empty?
        lookup_cost += sum_cost step unless step.index.sum_fields.empty?
        lookup_cost += avg_cost step unless step.index.avg_fields.empty?
        lookup_cost += groupby_cost step unless step.index.groupby_fields.empty?
        lookup_cost
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

      def prepare_delete_cost(step)
        return nil if step.state.nil?
        step.state.cardinality * @options[:prepare_delete_cost]
      end

      def prepare_insert_cost(step)
        return nil if step.state.nil?
        step.state.cardinality * @options[:prepare_insert_cost]
      end

      private

      def count_cost(step)
        count_fields = step.index.count_fields
        count_fields.map{|_| @options[:count_cost] * step.state.hash_cardinality}.sum()
      end

      def sum_cost(step)
        sum_fields = step.index.sum_fields
        sum_fields.map{|_| @options[:sum_cost] * step.state.hash_cardinality}.sum()
      end

      def avg_cost(step)
        avg_fields = step.index.count_fields
        avg_fields.map{|_| @options[:avg_cost] * step.state.hash_cardinality}.sum()
      end

      def groupby_cost(step)
        groupby_fields = step.index.groupby_fields
        groupby_fields.map{|_| @options[:groupby_cost] * step.state.hash_cardinality}.sum()
      end
    end
  end
end
