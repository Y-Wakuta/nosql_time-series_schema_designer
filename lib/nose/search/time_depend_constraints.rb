# frozen_string_literal: true

module NoSE
  module Search
    # Constraints which force indexes to be present to be used
    class TimeDependIndexPresenceConstraints < Constraint
      # Add constraint for indices being present
      def self.apply(problem)
        problem.indexes.each do |index|
          problem.queries.each_with_index do |query, q|
            (0...problem.timesteps).each do |ts|
              name = "q#{q}_#{index.key}_avail_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              constr = MIPPeR::Constraint.new problem.query_vars[index][query][ts] +
                                                problem.index_vars[index][ts] * -1,
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

    class TimeDependPrepareConstraints < Constraint
      def self.apply(problem)
        l = problem.index_vars.size.to_f
        problem.trees.each do |tree|
          tree.each do |plan|
            (1...problem.timesteps).each do |ts|
              prepare_var = problem.prepare_vars[tree.query].find{|key, _| key == plan}.last[ts]

              lookup_steps = plan.select{|step| step.is_a? Plans::IndexLookupPlanStep}
              current_plan = lookup_steps.reduce(MIPPeR::LinExpr) {|_, step| problem.index_vars[step.index][ts] * 1.0}
              former_plan = lookup_steps.reduce(MIPPeR::LinExpr){|_, step| problem.index_vars[step.index][ts - 1] * -1.0}

              constr = MIPPeR::Constraint.new(prepare_var * l + current_plan + former_plan, :>=, 0, name)
              problem.model << constr
            end
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
            # Find the index associated with the support query and make
            # the requirement of a plan conditional on this index
            index_var = if problem.data[:by_id_graph]
                          problem.index_vars[query.index.to_id_graph][timestep]
                        else
                          problem.index_vars[query.index]&.fetch(timestep)
                        end
            next if index_var.nil?

            constr = MIPPeR::Constraint.new constraint + index_var * -1.0,
                                            :>=, 0, name
          else
            constr = MIPPeR::Constraint.new constraint, :==, 1, name
          end

          problem.model << constr
        end
      end

      # Add complete query plan constraints
      def self.apply_query(query, q, problem)
        entities = query.join_order
        query_constraints_whole_time = (0...problem.timesteps).map do |_|
          Hash[entities.each_cons(2).map do |e, next_e|
            [[e, next_e], MIPPeR::LinExpr.new]
          end]
        end

        query_constraints_whole_time.each_with_index do |query_constraints, ts|
          first, last = setup_query_constraints(query_constraints, entities)

          problem.data[:costs][query].each do |index, (steps, _)|
            index_var = problem.query_vars[index][query][ts]
            construct_query_constraints(index, index_var, steps, entities, query_constraints, first, last)

            parent_index = steps.first.parent.parent_index
            next if parent_index.nil?
            parent_var = problem.query_vars[parent_index][query][ts]
            ensure_parent_index_available(index, index_var, parent_var, problem, q)
          end

          # Ensure we have exactly one index on each component of the query graph
          add_query_constraints query, q, query_constraints, problem, ts
        end
      end
    end
  end
end
