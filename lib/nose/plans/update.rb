# frozen_string_literal: true

module NoSE
  module Plans
    # A superclass for steps which modify indexes
    class UpdatePlanStep < PlanStep
      attr_reader :index, :prepare_update_cost_with_size
      attr_accessor :state

      def initialize(index, type, state = nil)
        super()
        @index = index
        @type = type

        return if state.nil?
        @state = state.dup
        @state.freeze
      end

      # :nocov:
      def to_color
        "#{super} #{@index.to_color} * #{@state.cardinality}"
      end
      # :nocov:

      # Two insert steps are equal if they use the same index
      def ==(other)
        other.instance_of?(self.class) && @index == other.index && \
          @type == other.instance_variable_get(:@type)
      end
      alias eql? ==

      def hash
        [@index, @type].hash
      end

      # calculate the cost of updates during CF creation
      # Since the time length of CF creation corresponding to the CF size,
      # multiple update cost with index.size with coefficient
      # At least this cost is smaller than normal update cost since this
      # updating-creating-cf does not executed for whole interval
      # As long as the creation time is shorter than the interval
      def calculate_update_prepare_cost(cost_model)
        index_creation_time = cost_model.method(('load_cost').to_sym).call @index
        @prepare_update_cost_with_size = (cost_model.method((subtype_name + '_cost').to_sym).call self) * index_creation_time
      end
    end

    # A step which inserts data into a given index
    class InsertPlanStep < UpdatePlanStep
      attr_reader :fields

      def initialize(index, state = nil, fields = Set.new)
        super index, :insert, state
        @fields = if fields.empty?
                    index.all_fields
                  else
                    fields.to_set & index.all_fields
                  end
        @fields += index.hash_fields + index.order_fields.to_set
      end
    end

    # A step which deletes data into a given index
    class DeletePlanStep < UpdatePlanStep
      def initialize(index, state = nil)
        super index, :delete, state
      end
    end
  end
end
