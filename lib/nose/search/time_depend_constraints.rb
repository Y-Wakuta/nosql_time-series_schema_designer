# frozen_string_literal: true

module NoSE
  module Search
    # Constraints which force indexes to be present to be used
    class TimeDependIndexPresenceConstraints < Constraint
      # Add constraint for indices being present
      def self.apply(problem)
        # constraint for Query or SupportQuery
        problem.queries.reject{|q| q.is_a? MigrateSupportQuery}.each_with_index do |query, q|
          problem.data[:costs][query].keys.uniq.each do |related_index|
            (0...problem.timesteps).each do |ts|
              name = "q#{q}_#{related_index.key}_avail_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              constr = MIPPeR::Constraint.new problem.query_vars[related_index][query][ts] +
                                                  problem.index_vars[related_index][ts] * -1,
                                              :<=, 0, name
              problem.model << constr
            end
          end
        end

        # constraint for MigrateSupportQuery
        # if the index is created in the migration process, indexes for migration are required
        (0...(problem.timesteps - 1)).each do |ts|
          problem.queries.select{|q| q.is_a? MigrateSupportQuery}.each do |ms_query|
            problem.data[:costs][ms_query].keys.uniq.each do |related_index|
              name = "ms_q_#{related_index.key}_avail_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              constr = MIPPeR::Constraint.new problem.prepare_vars[related_index][ms_query][ts] +
                                                  problem.index_vars[related_index][ts] * -1,
                                              :<=, 0, name
              problem.model << constr
            end
          end
        end
      end
    end

    class TimeDependCreationConstraints < Constraint
      def self.apply(problem)
        problem.indexes.each do |index|
          (1...problem.timesteps).each do |ts|
            constr = MIPPeR::Constraint.new(problem.migrate_vars[index][ts] * 1.0 +
                                                problem.index_vars[index][ts] * -1.0 +
                                                problem.index_vars[index][ts - 1] * 1.0,
                                            :>=, 0)
            problem.model << constr
          end
        end
      end
    end


    # The single constraint used to enforce a maximum storage cost
    class TimeDependSpaceConstraint < Constraint
      # Add space constraint if needed
      def self.apply(problem)
        return unless problem.data[:max_space].finite?

        fail 'Space constraint not supported when grouping by ID graph' \
          if problem.data[:by_id_graph]

        spaces = problem.total_size_each_timestep
        spaces.each do |space|
          constr = MIPPeR::Constraint.new space, :<=,
                                          problem.data[:max_space] * 1.0,
                                          'max_space'
          problem.model << constr
        end
      end
    end

    # Constraints that force each query to have an available plan
    class TimeDependCompletePlanConstraints < CompletePlanConstraints
      # Add the discovered constraints to the problem
      def self.add_query_constraints(query, q, constraints, problem, timestep)
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
              problem.model << constr_current
              problem.model << constr_next_timestep
              problem.model << constr_upper
            else
              constr = MIPPeR::Constraint.new constraint + index_var * -1.0,
                                              :==, 0, name
              problem.model << constr
            end
          else
            constr = MIPPeR::Constraint.new constraint, :==, 1, name
            problem.model << constr
          end
        end
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

      def self.time_depend_complete_plan_constraint(query, q, problem, query_plan_step_vars, timesteps)
        entities = query.join_order
        query_constraints_whole_time = (0...timesteps).map do |_|
          Hash[entities.each_cons(2).map do |e, next_e|
            [[e, next_e], MIPPeR::LinExpr.new]
          end]
        end

        query_constraints_whole_time.each_with_index do |query_constraints, ts|
          first, last = setup_query_constraints(query_constraints, entities)

          problem.data[:costs][query].each do |index, (steps, _)|
            index_var = query_plan_step_vars[index][query][ts]
            construct_query_constraints(index, index_var, steps, entities, query_constraints, first, last)

            parent_index = steps.first.parent.parent_index
            next if parent_index.nil?
            parent_var = query_plan_step_vars[parent_index][query][ts]
            ensure_parent_index_available(index, index_var, parent_var, problem, q)
          end

          # Ensure we have exactly one index on each component of the query graph
          add_query_constraints query, q, query_constraints, problem, ts
        end
      end

      # Add complete query plan constraints
      def self.apply_query(query, q, problem)
        return if query.is_a? MigrateSupportQuery
        time_depend_complete_plan_constraint(query, q, problem, problem.query_vars, problem.timesteps)
      end
    end

    class TimeDependPrepareConstraints < TimeDependCompletePlanConstraints
      def self.add_query_constraints(query, q, constraints, problem, timestep)
        constraints.each do |entities, constraint|
          name = "q#{q}_#{entities.map(&:name).join '_'}_#{timestep}" \
              if ENV['NOSE_LOG'] == 'debug'

          index_var = time_depend_index_var problem, query, timestep
          # if index_var is nil, the index is not used in any query plan
          next if index_var.nil?

          if timestep < (problem.timesteps - 1) # The SupportQuery maybe for updating migrate-preparing indexes
            next_timestep_migrate_var = problem.prepare_tree_vars[query.index][query]&.fetch(timestep)

            fail "if index_var exists, next_timestep_migrate_var also should exists" if next_timestep_migrate_var.nil?

            # TODO: even if index_var and next_timestep_migrate_var are 0, constraint can take 1
            constr_next_timestep = MIPPeR::Constraint.new constraint + next_timestep_migrate_var * -1.0,
                                                         :==, 0, name
            problem.model << constr_next_timestep
          end
        end
      end

      # Add complete query plan constraints
      def self.apply_query(query, q, problem)
        return unless query.is_a? MigrateSupportQuery
        time_depend_complete_plan_constraint(query, q, problem, problem.prepare_vars, problem.timesteps - 1)
      end
    end

    class TimeDependPrepareTreeConstraints < Constraint
      def self.apply_query(query, q, problem)
        return unless query.is_a?(MigrateSupportQuery)
        (0...(problem.timesteps - 1)).each do |ts|
          problem.migrate_prepare_plans.each do |index, query_trees|
            plan_expr = MIPPeR::LinExpr.new
            query_trees.keys.each do |query|
                plan_expr.add problem.prepare_tree_vars[index][query][ts]
            end
            constr = MIPPeR::Constraint.new plan_expr + problem.migrate_vars[index][ts + 1] * -1.0,
                                            :>=,  0, "prepare_tree_constr_#{index.key}"
            problem.model << constr
          end
        end
      end
    end
  end
end
