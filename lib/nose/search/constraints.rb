# frozen_string_literal: true

module NoSE
  module Search
    # Base class for constraints
    class Constraint
      # If this is not overridden, apply query-specific constraints
      # @return [void]
      def self.apply(problem)
        problem.queries.each_with_index do |query, q|
          apply_query query, q, problem
        end
      end

      # To be implemented in subclasses for query-specific constraints
      # @return [void]
      def self.apply_query(*_args)
      end
    end

    # Constraints which force indexes to be present to be used
    class IndexPresenceConstraints < Constraint
      # Add constraint for indices being present
      def self.apply(problem)
        problem.indexes.each do |index|
          problem.queries.each_with_index do |query, q|
            name = "q#{q}_#{index.key}_avail" if ENV['NOSE_LOG'] == 'debug'
            constr = MIPPeR::Constraint.new problem.query_vars[index][query] +
                                              problem.index_vars[index] * -1,
                                            :<=, 0, name
            problem.model << constr
          end
        end
      end
    end

    # The single constraint used to enforce a maximum storage cost
    class SpaceConstraint < Constraint
      # Add space constraint if needed
      def self.apply(problem)
        return unless problem.data[:max_space].finite?

        fail 'Space constraint not supported when grouping by ID graph' \
          if problem.data[:by_id_graph]

        space = problem.total_size
        constr = MIPPeR::Constraint.new space, :<=,
                                        problem.data[:max_space] * 1.0,
                                        'max_space'
        problem.model << constr
      end
    end

    # Constraints that force each query to have an available plan
    class CompletePlanConstraints < Constraint
      # Add the discovered constraints to the problem
      def self.add_query_constraints(query, q, constraints, problem)
        constraints.each do |entities, constraint|
          name = "q#{q}_#{entities.map(&:name).join '_'}" \
              if ENV['NOSE_LOG'] == 'debug'

          # If this is a support query, then we might not need a plan
          if query.is_a? SupportQuery
            # Find the index associated with the support query and make
            # the requirement of a plan conditional on this index
            index_var = if problem.data[:by_id_graph]
                          problem.index_vars[query.index.to_id_graph]
                        else
                          problem.index_vars[query.index]
                        end
            next if index_var.nil?

            constr = MIPPeR::Constraint.new constraint + index_var * -1.0,
                                            :==, 0, name
          else
            constr = MIPPeR::Constraint.new constraint, :==, 1, name
          end

          problem.model << constr
        end
      end

      # Add complete query plan constraints
      def self.apply_query(query, q, problem)
        entities = query.join_order
        query_constraints = Hash[entities.each_cons(2).map do |e, next_e|
          [[e, next_e], MIPPeR::LinExpr.new]
        end]

        # Add the sentinel entities at the end and beginning
        last = Entity.new '__LAST__'
        query_constraints[[entities.last, last]] = MIPPeR::LinExpr.new
        first = Entity.new '__FIRST__'
        query_constraints[[entities.first, first]] = MIPPeR::LinExpr.new

        problem.data[:costs][query].each do |index, (steps, _)|
          # All indexes should advance a step if possible unless
          # this is either the last step from IDs to entity
          # data or the first step going from data to IDs
          index_step = steps.first
          fail if entities.length > 1 && index.graph.size == 1 && \
                  !(steps.last.state.answered? ||
            index_step.parent.is_a?(Plans::RootPlanStep))

          # Join each step in the query graph
          index_var = problem.query_vars[index][query]
          index_entities = index.graph.entities.sort_by do |entity|
            entities.index entity
          end
          index_entities.each_cons(2) do |entity, next_entity|
            # Make sure the constraints go in the correct direction
            if query_constraints.key?([entity, next_entity])
              query_constraints[[entity, next_entity]] += index_var
            else
              query_constraints[[next_entity, entity]] += index_var
            end
          end

          # If this query has been answered, add the jump to the last step
          query_constraints[[entities.last, last]] += index_var \
            if steps.last.state.answered?

          # If this index is the first step, add this index to the beginning
          query_constraints[[entities.first, first]] += index_var \
            if index_step.parent.is_a?(Plans::RootPlanStep)

          # Ensure the previous index is available
          parent_index = index_step.parent.parent_index
          next if parent_index.nil?

          parent_var = problem.query_vars[parent_index][query]
          name = "q#{q}_#{index.key}_parent" if ENV['NOSE_LOG'] == 'debug'
          constr = MIPPeR::Constraint.new index_var * 1.0 + parent_var * -1.0,
                                          :<=, 0, name
          problem.model << constr
        end

        # Ensure we have exactly one index on each component of the query graph
        add_query_constraints query, q, query_constraints, problem
      end
    end

    # 各クエリにおいて、始点から１つのみ経路が作成されることを保証
    class OnePathConstraint < Constraint

      def self.start_one_path(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          start_edges = []
          edge_var.each do |from, edge|
            edge.each do |to, var|
              if from.is_a?(Plans::RootPlanStep)
                start_edges << var
              end
            end
          end
          start_paths = start_edges.map{|se| se * 1.0}.inject(:+)
          constr = MIPPeR::Constraint.new start_paths, :==, 1, "StartOnePathConstraint of #{query}"
          problem_graph.model << constr
        end
      end

      # guarantee only one path enter the last node for each query
      def self.last_one_path(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          edges_to_last = [] # クエリごとにある index について、その index を from にもつ edge が１つも無い場合に、そのノードに入っている edge をリストに追加。それらの変数の和が1になるようにする
          problem_graph.index_vars.map{|index, var| index}.each do |index|
            incoming_edges = []
            edge_var.each do |from, edge|
              edge.each do |to, var|
                if to.is_a?(Plans::IndexLookupPlanStep) and to.index == index
                  incoming_edges << var
                elsif from.is_a?(Plans::IndexLookupPlanStep) and from.index == index
                  incoming_edges = [] # if same edges out go from step that has the index, the step is not the last step
                end
              end
            end
            edges_to_last << incoming_edges unless incoming_edges.empty?
          end

          constr = MIPPeR::Constraint.new edges_to_last.flatten.map{|e| e * 1.0}.inject(:+), :==, 1, "LastOnePathConstraint of #{query}"
          problem_graph.model << constr
        end
      end

      def self.apply(problem_graph)
        start_one_path(problem_graph)
        last_one_path(problem_graph)
      end
    end

    # Guarantee that the number of nodes entering and leaving is the same
    class SameIOConstraint < Constraint

      def self.apply(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          problem_graph.get_indexes_by_query(query).each do |index|
            incoming_edges = MIPPeR::LinExpr.new
            outgoing_edges = MIPPeR::LinExpr.new
            edge_var.each do |from, edge|
              edge.each do |to, var|
                if to.is_a?(Plans::IndexLookupPlanStep) and to.index == index
                  incoming_edges += var * 1
                elsif from.is_a?(Plans::IndexLookupPlanStep) and from.index == index
                  outgoing_edges += var * (-1)
                end
              end
            end

            next if outgoing_edges.terms.size == 0 # if no edge go out from the index, the index is the last step of the plan

            constr = MIPPeR::Constraint.new incoming_edges + outgoing_edges , :==, 0, "io_same constraint for #{index.hash_str}"
            problem_graph.model << constr
          end
        end
      end
    end

    # guarantee that if one of incomming edge is used, the index is also used
    class PlanEdgeConstraints < Constraint
      def self.apply(problem_graph)
        edge_vars = problem_graph.edge_vars
        index_vars = problem_graph.index_vars

        whole_edge_num = edge_vars.size # the number which is larger than whole number of edges
        index_vars.each do |index, index_var|
          incoming_edge_vars = []
          edge_vars.each do |_, edge|
            edge.each do |_, var|
              var.each do |to , var|
                next unless to.is_a? Plans::IndexLookupPlanStep

                if to.index == index
                  incoming_edge_vars.append(var)
                end
              end
            end
          end
          incoming_edges_lin = incoming_edge_vars.map{|iev| iev * 1.0}.inject(:+)

          constr = MIPPeR::Constraint.new incoming_edges_lin + (index_var * (-whole_edge_num)), :<=, 0
          problem_graph.model << constr
        end
      end
    end
  end
end
