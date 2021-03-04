# frozen_string_literal: true

module NoSE
  module Search
    class EnumerableConstraint < Constraint
       def self.apply(problem)
        enumerate_constraints(problem).each {|c| problem.model << c}
      end
    end

    # Constraints which force indexes to be present to be used
    class TimeDependIndexPresenceConstraints < EnumerableConstraint
      # Add constraint for indices being present
      def self.enumerate_constraints(problem)
        # constraint for Query or SupportQuery
        problem_constraints = problem.queries.reject{|q| q.is_a? MigrateSupportQuery}.flat_map.with_index do |query, q|
          problem.data[:costs][query].keys.uniq.flat_map do |related_index|
            (0...problem.timesteps).map do |ts|
              name = "q#{q}_#{related_index.key}_avail_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              MIPPeR::Constraint.new problem.query_vars[related_index][query][ts] +
                                                problem.index_vars[related_index][ts] * -1,
                                              :<=, 0, name

            end
          end
        end

        # constraint for MigrateSupportQuery
        # if the index is created in the migration process, indexes for migration are required
        problem_constraints += (0...(problem.timesteps - 1)).flat_map do |ts|
          problem.queries.select{|q| q.is_a? MigrateSupportQuery}.flat_map do |ms_query|
            problem.data[:costs][ms_query].keys.uniq.flat_map do |related_index|
              name = "ms_q_#{related_index.key}_avail_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              MIPPeR::Constraint.new problem.prepare_vars[related_index][ms_query][ts] +
                                                problem.index_vars[related_index][ts] * -1,
                                              :<=, 0, name
            end
          end
        end
        problem_constraints
      end
    end

     class TimeDependCreationConstraints < EnumerableConstraint
      def self.enumerate_constraints(problem)
        problem_constraints = []
        problem.indexes.each do |index|
          (1...problem.timesteps).each do |ts|
            name = "creation_#{index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
            constr = MIPPeR::Constraint.new(problem.migrate_vars[index][ts] * 1.0 +
                                              problem.index_vars[index][ts] * -1.0 +
                                              problem.index_vars[index][ts - 1] * 1.0,
                                            :>=, 0, name)
            problem_constraints.append constr

            name = "creation_upper_#{index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
            constr_upper = MIPPeR::Constraint.new(problem.migrate_vars[index][ts] * 1.0 +
                                                    problem.index_vars[index][ts] * -1.0,
                                                  :<=, 0, name)
            problem_constraints.append constr_upper
          end
        end
        problem_constraints
      end
    end

    class TimeDependIndexesWithoutPreparePlanNotMigrated < EnumerableConstraint
      def self.enumerate_constraints(problem)
        problem_constraints = []
        # migrate plan and its corresponding variables are not created for Support Queries.
        # Therefore, no cost is calculated for the migration.
        # So, force migrates vars to be 0 that do not have to migrate plans.
        (problem.indexes - problem.trees.select{|t| t.query.instance_of? Query}.flat_map{|t| t.flat_map(&:indexes)}.uniq)
          .each do |support_indexes|
          (1...problem.timesteps).each do |ts|
            constr_upper = MIPPeR::Constraint.new(problem.migrate_vars[support_indexes][ts] * 1.0,
                                                  :==, 0)
            problem_constraints.append constr_upper
          end
        end
        problem_constraints
      end
    end

    # The single constraint used to enforce a maximum storage cost
    class TimeDependSpaceConstraint < EnumerableConstraint
      def self.enumerate_constraints(problem)
        return [] unless problem.data[:max_space].finite?

        fail 'Space constraint not supported when grouping by ID graph' \
          if problem.data[:by_id_graph]

        problem_constraints = []
        normalizer = problem.get_index_size_normalizer
        spaces = problem.total_size_each_timestep
        spaces.each_with_index do |space, idx|
          constr = MIPPeR::Constraint.new space, :<=,
                                          (problem.data[:max_space] / normalizer.to_f) * 1.0,
                                          "max_space_#{idx}"
          problem_constraints.append constr
        end
        problem_constraints
      end
    end

    # Constraints that force each query to have an available plan
    class TimeDependCompletePlanConstraints < CompletePlanConstraints
      # Add the discovered constraints to the problem
      def self.get_query_constraints(query, q, constraints, problem, timestep)
        problem_constraints = []
        constraints.each do |entities, constraint|
          name = "q#{q}_#{entities.map(&:name).join '_'}_#{timestep}" \
              if ENV['NOSE_LOG'] == 'debug'

          # If this is a support query, then we might not need a plan
          if query.is_a? SupportQuery
            index_var = time_depend_index_var problem, query, timestep
            # if index_var is nil, the index is not used in any query plan
            next if index_var.nil?

            if timestep < (problem.timesteps - 1) # The SupportQuery maybe for updating migrate-preparing indexes
              next_timestep_migrate_var = problem.migrate_vars[query.index]&.fetch(timestep + 1)

              fail "if index_var exists, next_timestep_migrate_var also should exists" if next_timestep_migrate_var.nil?

              # TODO: even if index_var and next_timestep_migrate_var are 0, constraint can take 1
              constr_current = MIPPeR::Constraint.new constraint + index_var * -1.0,
                                                      :>=, 0, name
              constr_next_timestep= MIPPeR::Constraint.new constraint + next_timestep_migrate_var * -1.0,
                                                           :>=, 0, name
              constr_upper = MIPPeR::Constraint.new constraint, :<=, 1, name
              problem_constraints.append constr_current
              problem_constraints.append constr_next_timestep
              problem_constraints.append constr_upper
            else
              constr = MIPPeR::Constraint.new constraint + index_var * -1.0,
                                              :==, 0, name
              problem_constraints.append constr
            end
          else
            constr = MIPPeR::Constraint.new constraint, :==, 1, name
            problem_constraints.append constr
          end
        end
        problem_constraints
      end

      # Find the index associated with the support query and make
      # the requirement of a plan conditional on this index
      def self.time_depend_index_var(problem, query, timestep)
        index_var = if problem.data[:by_id_graph]
                      problem.index_vars[query.index.to_id_graph][timestep]
                    else
                      problem.index_vars[query.index]&.fetch(timestep)
                    end
        index_var
      end

       # Ensure the previous index is available
      def self.ensure_parent_index_available(index, index_var, parent_var, problem, q)
        name = "q#{q}_#{index.key}_parent" if ENV['NOSE_LOG'] == 'debug'
        MIPPeR::Constraint.new index_var * 1.0 + parent_var * -1.0,
                                        :<=, 0, name
      end

      def self.time_depend_complete_plan_constraint(query, q, problem, query_plan_step_vars, timesteps)
        problem_constraints = []
        entities = query.join_order
        query_constraints_whole_time = (0...timesteps).map do |_|
          Hash[entities.each_cons(2).map do |e, next_e|
            [[e, next_e], MIPPeR::LinExpr.new]
          end]
        end

        query_constraints_whole_time.flat_map.with_index do |query_constraints, ts|
          first, last = setup_query_constraints(query_constraints, entities)

          problem.data[:costs][query].each do |index, (steps, _)|
            index_var = query_plan_step_vars[index][query][ts]
            construct_query_constraints(index, index_var, steps, entities, query_constraints, first, last)

            parent_index = steps.first.parent.parent_index
            next if parent_index.nil?
            parent_var = query_plan_step_vars[parent_index][query][ts]
            problem_constraints.append ensure_parent_index_available(index, index_var, parent_var, problem, q)
          end

          # Ensure we have exactly one index on each component of the query graph
          problem_constraints += get_query_constraints(query, q, query_constraints, problem, ts)
        end
        problem_constraints
      end

      def self.enumerate_constraints(problem)
        Parallel.flat_map(problem.queries.reject{|q| q.is_a? MigrateSupportQuery}, in_threads: Parallel.processor_count / 2) do |query, q|
          time_depend_complete_plan_constraint(query, q, problem, problem.query_vars, problem.timesteps).map(&:clone)
        end
      end

      def self.apply(problem)
        enumerate_constraints(problem).each {|c| problem.model << c}
      end
    end

    class TimeDependPrepareConstraints < TimeDependCompletePlanConstraints
      def self.get_query_constraints(query, q, constraints, problem, timestep)
        problem_constraints = []
        constraints.each do |entities, constraint|

          index_var = time_depend_index_var problem, query, timestep
          # if index_var is nil, the index is not used in any query plan
          next if index_var.nil?

          if timestep < (problem.timesteps - 1) # The SupportQuery maybe for updating migrate-preparing indexes
            next_timestep_migrate_var = problem.prepare_tree_vars[query.index][query]&.fetch(timestep)

            fail "if index_var exists, next_timestep_migrate_var also should exists" if next_timestep_migrate_var.nil?

            name = "q#{q}_#{entities.map(&:name).join '_'}_#{timestep}" \
              if ENV['NOSE_LOG'] == 'debug'
            # TODO: even if index_var and next_timestep_migrate_var are 0, constraint can take 1
            constr_next_timestep = MIPPeR::Constraint.new constraint + next_timestep_migrate_var * -1.0,
                                                          :==, 0, name
            problem_constraints.append constr_next_timestep
          end
        end
        problem_constraints
      end

      def self.enumerate_constraints(problem)
        Parallel.flat_map(problem.queries.select{|q| q.is_a? MigrateSupportQuery}.each_with_index, in_threads: Parallel.processor_count / 2) do |query, q|
          time_depend_complete_plan_constraint(query, q, problem, problem.prepare_vars, problem.timesteps - 1)
        end
      end
    end

    class TimeDependPrepareTreeConstraints < EnumerableConstraint
      def self.enumerate_constraints(problem)
        problem_constraints = []
        (0...(problem.timesteps - 1)).each do |ts|
          problem.migrate_prepare_plans.each do |index, query_trees|
            plan_expr = MIPPeR::LinExpr.new
            query_trees.keys.each do |query|
              plan_expr.add problem.prepare_tree_vars[index][query][ts]
            end
            constr = MIPPeR::Constraint.new plan_expr + problem.migrate_vars[index][ts + 1] * -1.0,
                                            :==,  0, "prepare_tree_constr_#{index.key}"
            problem_constraints.append constr
          end
        end
        problem_constraints
      end
    end

     class TimeDependIndexCreatedAtUsedTimeStepConstraints < EnumerableConstraint
      def self.enumerate_constraints(problem)
        problem_constraints = []
        problem.query_vars.each do |index, q_vars|
           q_vars.each do |_, q_var|
             (1...problem.timesteps).each do |ts|
               constr = MIPPeR::Constraint.new q_var[ts] * 1.0 + problem.migrate_vars[index][ts] * -1.0, :>=,
                                               0, "index_created_at_used_ts_#{index.key}"
               problem_constraints.append constr
             end
           end
        end
        problem_constraints
      end
     end
  end
end
