module NoSE
  module Plans

    class MigratePlan
      attr_accessor :query, :start_time, :end_time, :obsolete_plan, :new_plan, :prepare_plans
      def initialize(query, start_time, obsolete_plan, new_plan)
        @query = query
        @start_time = start_time
        @end_time = start_time + 1
        @obsolete_plan = obsolete_plan
        @new_plan = new_plan
        @prepare_plans = []
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

    class MigratePreparePlan
      attr_accessor :index, :query_plan, :timestep
      def initialize(index, query_plan, timestep)
        @index = index
        @query_plan = query_plan
        @timestep = timestep
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

