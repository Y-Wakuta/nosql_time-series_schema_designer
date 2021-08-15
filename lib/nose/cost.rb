# frozen_string_literal: true

module NoSE
  # Cost models for steps of backend statement excution
  module Cost
    # Cost model for a backend database
    class Cost
      include Listing
      include Supertype

      def initialize(**options)
        @options = options
      end

      # The cost of filtering intermediate results
      # @return [Fixnum]
      def filter_cost(_step)
        # Assume this has no cost and the cost is captured in the fact that we
        # have to retrieve more data earlier. All this does is skip records.
        0
      end

      # The cost of aggregating the intermediate results at the last step
      # @return [Fixnum]
      def aggregation_cost(_step)
        # aggregation is done by sorting and grouping
        # estimate aggregation function (e.g. sums(), maxes(), avgs(), counts()) cost to be 0.
        # estimate GROUP BY cost is proportional to the number of GROUP BY fields and sort_cost of each GROUP BY field.
        _step.groupby.size * sort_cost(_step)
      end

      # The cost of limiting a result set
      # @return [Fixnum]
      def limit_cost(_step)
        # This is basically free since we just discard data
        0
      end

      # The cost of sorting a set of results
      # @return [Fixnum]
      def sort_cost(_step)
        # TODO: Find some estimate of sort cost
        #       This could be partially captured by the fact that sort + limit
        #       effectively removes the limit
        1.0
      end

      # The cost of performing a lookup via an index
      # @return [Fixnum]
      def index_lookup_cost(_step)
        fail NotImplementedError, 'Must be implemented in a subclass'
      end

      # The cost of performing a deletion from an index
      # @return [Fixnum]
      def delete_cost(_step)
        fail NotImplementedError, 'Must be implemented in a subclass'
      end

      # The cost of performing an insert into an index
      # @return [Fixnum]
      def insert_cost(_step)
        fail NotImplementedError, 'Must be implemented in a subclass'
      end

      # This is here for debugging purposes because we need a cost
      # @return [Fixnum]
      def pruned_cost(_step)
        0
      end

      def extract_cost(step)
        fail NotImplementedError, 'Must be implemented in a subclass'
      end

      def load_cost(index)
        fail NotImplementedError, 'Must be implemented in a subclass'
      end
    end
  end
end

require_relative 'cost/cassandra'
require_relative 'cost/cassandra_io'
require_relative 'cost/entity_count'
require_relative 'cost/field_size'
require_relative 'cost/request_count'
