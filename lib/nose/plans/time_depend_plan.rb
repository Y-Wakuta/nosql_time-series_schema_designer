module NoSE
  module Plans

    class MigratePlan
      attr_accessor :query, :start_time, :end_time, :obsolete_plan, :new_plan
      def initialize(query, start_time, end_time, obsolete_plan, new_plan)
        @query = query
        @start_time = start_time
        @end_time = end_time
        @obsolete_plan = obsolete_plan
        @new_plan = new_plan
      end
    end

    class TimeDependPlan
      attr_accessor :query, :plans
      def initialize(query, plans)
        @query = query
        @plans = plans
      end
    end

    class TimeDependSupportPlanEachTimestep
      attr_accessor :plans
      def initialize(plans)
        @plans = plans
      end
    end

    class TimeDependUpdatePlanEachTimestep
      attr_accessor :plans
      def initialize(plans)
        @plans = plans
      end
    end

    class TimeDependUpdatePlan
      attr_accessor :statement, :plans_all_timestep

      def initialize(statement, plans_all_timestep)
        @statement = statement
        @plans_all_timestep = plans_all_timestep.map{|pat| TimeDependUpdatePlanEachTimestep.new(pat)}
      end
    end
  end
end

