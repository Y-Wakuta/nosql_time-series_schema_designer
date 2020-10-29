# frozen_string_literal: true

module NoSE
  module Search
    class TimeDependIndexFixConstraints < Constraint
      def self.apply(problem)
        return unless (problem.ts_indexes.has_key?(problem.start_ts) and problem.ts_indexes.has_key?(problem.end_ts))

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
        chosen_indexes = problem.ts_indexes.values.reduce(&:+)
        (problem.indexes.to_set - chosen_indexes.to_set).each do |un_chosen_idxes|
          (0...problem.timesteps).each do |ts|
            constr = MIPPeR::Constraint.new problem.index_vars[un_chosen_idxes][ts] * 1.0,
                                            :==, 0, name
            problem.model << constr
          end
        end
      end
    end
  end
end
