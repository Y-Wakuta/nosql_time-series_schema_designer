# frozen_string_literal: true

module NoSE
  module Search
    class TimeDependIndexFixConstraints < Constraint
      def self.apply(problem)
        return unless (problem.ts_indexes.has_key?(problem.start_ts) and problem.ts_indexes.has_key?(problem.end_ts))

        #tss = [problem.start_ts, problem.middle_ts, problem.end_ts]
        tss = [problem.start_ts, problem.end_ts]
        tss.each do |ts|
          problem.ts_indexes[ts].each do |idx|
            name = "#{idx.key}_fixed_#{ts}" if ENV['NOSE_LOG'] == 'debug'
            constr = MIPPeR::Constraint.new problem.index_vars[idx][tss.index(ts)] * 1.0,
                                            :==, 1, name
            problem.model << constr
          end
        end
      end
    end

    class TimeDependIndexWholeFixConstraints < Constraint
      def self.apply(problem)
        problem.ts_indexes.each do |ts, indexes|
          indexes.each do |index|
            constr = MIPPeR::Constraint.new problem.index_vars[index][ts] * 1.0,
                                            :==, 1, name
            problem.model << constr
          end
        end
      end
    end

    class TimeDependQueryPlanFixConstraints < Constraint
      def self.apply(problem)
        return unless (problem.ts_indexes.has_key?(problem.start_ts) and problem.ts_indexes.has_key?(problem.end_ts))
        #tss = [problem.start_ts, problem.middle_ts, problem.end_ts]
        tss = [problem.start_ts, problem.end_ts]
        tss.each do |ts|
          problem.queries.select{|q| q.instance_of? Query}.each do |query|
            problem.ts_query_indexes[ts][query].each do |idx|
              name = "query_#{idx.key}_fixed_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              constr = MIPPeR::Constraint.new problem.query_vars[idx][query][tss.index(ts)] * 1,
                                              :==, 1, name
              problem.model << constr
            end
          end
        end
      end
    end

  end
end
