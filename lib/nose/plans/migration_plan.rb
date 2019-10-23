module NoSE
  module Plans
    class MigratePlan
      attr_reader :query, :start_time, :end_time, :obsolete_plan, :new_plan
      def initialize(query, start_time, end_time, obsolete_plan, new_plan)
        @query = query
        @start_time = start_time
        @end_time = end_time
        @obsolete_plan = obsolete_plan
        @new_plan = new_plan
      end
    end
  end
end
