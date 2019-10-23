module NoSE
  module Plans
    class TimeDependPlan
      attr_reader :query, :plans
      def initialize(query, plans)
        @query = query
        @plans = plans
      end
    end

    class TimeDependUpdatePlan
      attr_reader :statement, :plans_all_timestep

      def initialize(statement, plans_all_timestep)
        @statement = statement
        @plans_all_timestep = plans_all_timestep
      end
    end
  end
end

